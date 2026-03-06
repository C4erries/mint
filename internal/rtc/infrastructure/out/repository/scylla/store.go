package scyllarepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/scylladb/gocqlx/v3/table"
)

const (
	defaultPort        = 9042
	defaultConsistency = "quorum"
)

const (
	roomsTable             = "rtc_voice_rooms"
	bindingsTable          = "rtc_voice_channel_bindings"
	processedCommandsTable = "rtc_processed_commands"
	outboxTable            = "rtc_outbox"
)

var (
	roomsModel = table.New(table.Metadata{
		Name: roomsTable,
		Columns: []string{
			"room_id",
			"workspace_id",
			"channel_id",
			"active",
			"created_at",
			"updated_at",
			"participants_json",
		},
		PartKey: []string{"room_id"},
	})
	bindingsModel = table.New(table.Metadata{
		Name: bindingsTable,
		Columns: []string{
			"workspace_id",
			"channel_id",
			"room_id",
			"bound_at",
			"updated_at",
		},
		PartKey: []string{"workspace_id"},
		SortKey: []string{"channel_id"},
	})
	processedCommandsModel = table.New(table.Metadata{
		Name: processedCommandsTable,
		Columns: []string{
			"command_id",
			"processed_at",
		},
		PartKey: []string{"command_id"},
	})
	outboxModel = table.New(table.Metadata{
		Name: outboxTable,
		Columns: []string{
			"event_id",
			"event_type",
			"command_id",
			"correlation_id",
			"causation_id",
			"message_id",
			"occurred_at",
			"workspace_id",
			"channel_id",
			"room_id",
			"actor_id",
			"schema_version",
			"payload_json",
			"published",
		},
		PartKey: []string{"event_id"},
	})
)

type roomRow struct {
	RoomID           string    `db:"room_id"`
	WorkspaceID      string    `db:"workspace_id"`
	ChannelID        string    `db:"channel_id"`
	Active           bool      `db:"active"`
	CreatedAt        time.Time `db:"created_at"`
	UpdatedAt        time.Time `db:"updated_at"`
	ParticipantsJSON string    `db:"participants_json"`
}

type bindingRow struct {
	WorkspaceID string    `db:"workspace_id"`
	ChannelID   string    `db:"channel_id"`
	RoomID      string    `db:"room_id"`
	BoundAt     time.Time `db:"bound_at"`
	UpdatedAt   time.Time `db:"updated_at"`
}

type processedCommandRow struct {
	CommandID   string    `db:"command_id"`
	ProcessedAt time.Time `db:"processed_at"`
}

type outboxRow struct {
	EventID       string    `db:"event_id"`
	EventType     string    `db:"event_type"`
	CommandID     string    `db:"command_id"`
	CorrelationID string    `db:"correlation_id"`
	CausationID   string    `db:"causation_id"`
	MessageID     string    `db:"message_id"`
	OccurredAt    time.Time `db:"occurred_at"`
	WorkspaceID   string    `db:"workspace_id"`
	ChannelID     string    `db:"channel_id"`
	RoomID        string    `db:"room_id"`
	ActorID       string    `db:"actor_id"`
	SchemaVersion int       `db:"schema_version"`
	PayloadJSON   string    `db:"payload_json"`
	Published     bool      `db:"published"`
}

// SessionFactory wraps gocql session constructor for easier testing.
type SessionFactory interface {
	CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error)
}

type defaultSessionFactory struct{}

func (defaultSessionFactory) CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error) {
	return cluster.CreateSession()
}

// Options configures Scylla-backed rtc repository.
type Options struct {
	Hosts            []string
	Port             int
	Keyspace         string
	Consistency      string
	AutoCreateSchema bool
	Now              func() time.Time
	SessionFactory   SessionFactory
}

// Store provides write/read/outbox persistence backed by ScyllaDB.
type Store struct {
	rawSession *gocql.Session
	session    gocqlx.Session
	now        func() time.Time
}

func NewStore(options Options) (*Store, error) {
	hosts := normalizeHosts(options.Hosts)
	if len(hosts) == 0 {
		return nil, fmt.Errorf("scylla hosts are required")
	}

	if strings.TrimSpace(options.Keyspace) == "" {
		return nil, fmt.Errorf("scylla keyspace is required")
	}

	if options.Port <= 0 {
		options.Port = defaultPort
	}

	if options.Consistency == "" {
		options.Consistency = defaultConsistency
	}

	if options.Now == nil {
		options.Now = time.Now
	}

	if options.SessionFactory == nil {
		options.SessionFactory = defaultSessionFactory{}
	}

	consistency, err := parseConsistency(options.Consistency)
	if err != nil {
		return nil, err
	}

	if options.AutoCreateSchema {
		if err := ensureKeyspace(options, hosts, consistency); err != nil {
			return nil, err
		}
	}

	rawSession, err := createKeyspaceSession(options, hosts, consistency)
	if err != nil {
		return nil, err
	}

	if options.AutoCreateSchema {
		if err = ensureSchema(context.Background(), rawSession); err != nil {
			rawSession.Close()
			return nil, err
		}
	}

	wrappedSession, err := gocqlx.WrapSession(rawSession, nil)
	if err != nil {
		rawSession.Close()
		return nil, fmt.Errorf("wrap scylla session: %w", err)
	}

	return &Store{
		rawSession: rawSession,
		session:    wrappedSession,
		now:        options.Now,
	}, nil
}

func ensureKeyspace(options Options, hosts []string, consistency gocql.Consistency) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = options.Port
	cluster.Consistency = consistency

	session, err := options.SessionFactory.CreateSession(cluster)
	if err != nil {
		return fmt.Errorf("create scylla admin session: %w", err)
	}
	defer session.Close()

	statement := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
		options.Keyspace,
	)

	if execErr := session.Query(statement).Exec(); execErr != nil {
		return fmt.Errorf("create scylla keyspace: %w", execErr)
	}

	return nil
}

func createKeyspaceSession(options Options, hosts []string, consistency gocql.Consistency) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = options.Port
	cluster.Keyspace = options.Keyspace
	cluster.Consistency = consistency

	session, err := options.SessionFactory.CreateSession(cluster)
	if err != nil {
		return nil, fmt.Errorf("create scylla keyspace session: %w", err)
	}

	return session, nil
}

func ensureSchema(ctx context.Context, session *gocql.Session) error {
	statements := []string{
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				room_id text PRIMARY KEY,
				workspace_id text,
				channel_id text,
				active boolean,
				created_at timestamp,
				updated_at timestamp,
				participants_json text
			)`, roomsTable),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				workspace_id text,
				channel_id text,
				room_id text,
				bound_at timestamp,
				updated_at timestamp,
				PRIMARY KEY ((workspace_id), channel_id)
			)`, bindingsTable),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				command_id text PRIMARY KEY,
				processed_at timestamp
			)`, processedCommandsTable),
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				event_id text PRIMARY KEY,
				event_type text,
				command_id text,
				correlation_id text,
				causation_id text,
				message_id text,
				occurred_at timestamp,
				workspace_id text,
				channel_id text,
				room_id text,
				actor_id text,
				schema_version int,
				payload_json text,
				published boolean
			)`, outboxTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s_published_idx ON %s (published)", outboxTable, outboxTable),
	}

	for _, statement := range statements {
		if err := session.Query(statement).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("execute schema statement: %w", err)
		}
	}

	return nil
}

func (s *Store) Close() error {
	if s == nil || s.rawSession == nil {
		return nil
	}

	s.rawSession.Close()
	return nil
}

func (s *Store) WithTx(ctx context.Context, fn func(tx application.VoiceRoomWriteTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx := newTransaction(s)
	if err := fn(tx); err != nil {
		return err
	}

	return tx.commit(ctx)
}

func (s *Store) GetVoiceRoomState(ctx context.Context, workspaceID string, channelID string) (application.VoiceRoomState, error) {
	binding, err := s.getBinding(ctx, workspaceID, channelID)
	if err != nil {
		return application.VoiceRoomState{}, err
	}

	room, err := s.getRoom(ctx, binding.RoomID)
	if err != nil {
		return application.VoiceRoomState{}, err
	}

	return application.VoiceRoomState{
		RoomID:            room.ID,
		WorkspaceID:       room.WorkspaceID,
		ChannelID:         room.ChannelID,
		Active:            room.Active,
		ParticipantCount:  room.ActiveParticipantCount(),
		LastStateChangeAt: room.UpdatedAt,
	}, nil
}

func (s *Store) ListVoiceParticipants(ctx context.Context, roomID string) ([]application.VoiceParticipant, error) {
	room, err := s.getRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}

	participants := room.Participants()
	result := make([]application.VoiceParticipant, 0, len(participants))

	for _, participant := range participants {
		result = append(result, application.VoiceParticipant{
			UserID:          participant.UserID,
			JoinedAt:        participant.JoinedAt,
			LeftAt:          participant.LeftAt,
			MicrophoneMuted: participant.MicrophoneMuted,
			CameraEnabled:   participant.CameraEnabled,
		})
	}

	return result, nil
}

func (s *Store) GetVoiceChannelBinding(ctx context.Context, workspaceID string, channelID string) (application.VoiceChannelBindingView, error) {
	binding, err := s.getBinding(ctx, workspaceID, channelID)
	if err != nil {
		return application.VoiceChannelBindingView{}, err
	}

	return application.VoiceChannelBindingView{
		WorkspaceID: binding.WorkspaceID,
		ChannelID:   binding.ChannelID,
		RoomID:      binding.RoomID,
		UpdatedAt:   binding.UpdatedAt,
	}, nil
}

func (s *Store) ListUnpublished(ctx context.Context, limit int) ([]application.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}

	stmt, names := qb.Select(outboxTable).
		Columns(
			"event_id",
			"event_type",
			"command_id",
			"correlation_id",
			"causation_id",
			"message_id",
			"occurred_at",
			"workspace_id",
			"channel_id",
			"room_id",
			"actor_id",
			"schema_version",
			"payload_json",
		).
		Where(qb.Eq("published")).
		Limit(uint(limit)).
		AllowFiltering().
		ToCql()

	rows := make([]outboxRow, 0, limit)
	err := s.session.Query(stmt, names).
		BindMap(qb.M{"published": false}).
		WithContext(ctx).
		SelectRelease(&rows)
	if err != nil {
		return nil, fmt.Errorf("select unpublished outbox rows: %w", err)
	}

	messages := make([]application.OutboxMessage, 0, len(rows))
	for _, row := range rows {
		payload := map[string]string{}
		if row.PayloadJSON != "" {
			if unmarshalErr := json.Unmarshal([]byte(row.PayloadJSON), &payload); unmarshalErr != nil {
				return nil, fmt.Errorf("decode outbox payload: %w", unmarshalErr)
			}
		}

		messages = append(messages, application.OutboxMessage{
			EventID:       row.EventID,
			EventType:     row.EventType,
			CommandID:     row.CommandID,
			CorrelationID: row.CorrelationID,
			CausationID:   row.CausationID,
			MessageID:     row.MessageID,
			OccurredAt:    row.OccurredAt,
			WorkspaceID:   row.WorkspaceID,
			ChannelID:     row.ChannelID,
			RoomID:        row.RoomID,
			ActorID:       row.ActorID,
			SchemaVersion: row.SchemaVersion,
			Payload:       payload,
		})
	}

	return messages, nil
}

func (s *Store) MarkPublished(ctx context.Context, eventID string) error {
	if eventID == "" {
		return domain.ErrInvalidIdentifier
	}

	stmt, names := qb.Update(outboxTable).
		Set("published").
		Where(qb.Eq("event_id")).
		ToCql()

	if err := s.session.Query(stmt, names).
		BindMap(qb.M{"event_id": eventID, "published": true}).
		WithContext(ctx).
		ExecRelease(); err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}

	return nil
}

func (s *Store) getRoom(ctx context.Context, roomID string) (*domain.VoiceRoom, error) {
	row := roomRow{RoomID: roomID}
	loaded := roomRow{}

	err := s.session.Query(roomsModel.Get()).
		BindStruct(row).
		WithContext(ctx).
		GetRelease(&loaded)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrVoiceRoomNotFound
		}

		return nil, fmt.Errorf("query room: %w", err)
	}

	participants := make([]domain.VoiceParticipantSession, 0)
	if loaded.ParticipantsJSON != "" {
		if err = json.Unmarshal([]byte(loaded.ParticipantsJSON), &participants); err != nil {
			return nil, fmt.Errorf("decode room participants: %w", err)
		}
	}

	return rebuildRoom(roomSnapshot{
		RoomID:       loaded.RoomID,
		WorkspaceID:  loaded.WorkspaceID,
		ChannelID:    loaded.ChannelID,
		Active:       loaded.Active,
		CreatedAt:    loaded.CreatedAt,
		UpdatedAt:    loaded.UpdatedAt,
		Participants: participants,
	})
}

func (s *Store) saveRoom(ctx context.Context, room *domain.VoiceRoom) error {
	if room == nil {
		return domain.ErrInvalidIdentifier
	}

	participantsJSON, err := json.Marshal(room.Participants())
	if err != nil {
		return fmt.Errorf("encode room participants: %w", err)
	}

	row := roomRow{
		RoomID:           room.ID,
		WorkspaceID:      room.WorkspaceID,
		ChannelID:        room.ChannelID,
		Active:           room.Active,
		CreatedAt:        room.CreatedAt,
		UpdatedAt:        room.UpdatedAt,
		ParticipantsJSON: string(participantsJSON),
	}

	if err = s.session.Query(roomsModel.Insert()).
		BindStruct(row).
		WithContext(ctx).
		ExecRelease(); err != nil {
		return fmt.Errorf("save room: %w", err)
	}

	return nil
}

func (s *Store) getBinding(ctx context.Context, workspaceID string, channelID string) (*domain.VoiceChannelBinding, error) {
	row := bindingRow{WorkspaceID: workspaceID, ChannelID: channelID}
	loaded := bindingRow{}

	err := s.session.Query(bindingsModel.Get()).
		BindStruct(row).
		WithContext(ctx).
		GetRelease(&loaded)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrVoiceChannelBindingNotFound
		}

		return nil, fmt.Errorf("query channel binding: %w", err)
	}

	binding, err := domain.NewVoiceChannelBinding(loaded.WorkspaceID, loaded.ChannelID, loaded.RoomID, loaded.BoundAt)
	if err != nil {
		return nil, fmt.Errorf("rebuild channel binding: %w", err)
	}

	binding.UpdatedAt = loaded.UpdatedAt
	return binding, nil
}

func (s *Store) saveBinding(ctx context.Context, binding *domain.VoiceChannelBinding) error {
	if binding == nil {
		return domain.ErrInvalidIdentifier
	}

	row := bindingRow{
		WorkspaceID: binding.WorkspaceID,
		ChannelID:   binding.ChannelID,
		RoomID:      binding.RoomID,
		BoundAt:     binding.BoundAt,
		UpdatedAt:   binding.UpdatedAt,
	}

	if err := s.session.Query(bindingsModel.Insert()).
		BindStruct(row).
		WithContext(ctx).
		ExecRelease(); err != nil {
		return fmt.Errorf("save channel binding: %w", err)
	}

	return nil
}

func (s *Store) appendOutbox(ctx context.Context, message application.OutboxMessage) error {
	payloadJSON, err := json.Marshal(message.Payload)
	if err != nil {
		return fmt.Errorf("encode outbox payload: %w", err)
	}

	row := outboxRow{
		EventID:       message.EventID,
		EventType:     message.EventType,
		CommandID:     message.CommandID,
		CorrelationID: message.CorrelationID,
		CausationID:   message.CausationID,
		MessageID:     message.MessageID,
		OccurredAt:    message.OccurredAt,
		WorkspaceID:   message.WorkspaceID,
		ChannelID:     message.ChannelID,
		RoomID:        message.RoomID,
		ActorID:       message.ActorID,
		SchemaVersion: message.SchemaVersion,
		PayloadJSON:   string(payloadJSON),
		Published:     false,
	}

	if err = s.session.Query(outboxModel.Insert()).
		BindStruct(row).
		WithContext(ctx).
		ExecRelease(); err != nil {
		return fmt.Errorf("append outbox message: %w", err)
	}

	return nil
}

func (s *Store) isCommandProcessed(ctx context.Context, commandID string) (bool, error) {
	row := processedCommandRow{CommandID: commandID}
	loaded := processedCommandRow{}

	err := s.session.Query(processedCommandsModel.Get()).
		BindStruct(row).
		WithContext(ctx).
		GetRelease(&loaded)
	if err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("query processed command: %w", err)
	}

	return true, nil
}

func (s *Store) markCommandProcessed(ctx context.Context, commandID string) error {
	stmt, names := qb.Insert(processedCommandsTable).
		Columns("command_id", "processed_at").
		Unique().
		ToCql()

	applied, err := s.session.Query(stmt, names).
		BindMap(qb.M{
			"command_id":   commandID,
			"processed_at": s.now().UTC(),
		}).
		WithContext(ctx).
		ExecCASRelease()
	if err != nil {
		return fmt.Errorf("mark command processed: %w", err)
	}

	if !applied {
		return nil
	}

	return nil
}

type transaction struct {
	store *Store

	loadedRooms    map[string]*domain.VoiceRoom
	dirtyRooms     map[string]struct{}
	loadedBindings map[string]*domain.VoiceChannelBinding
	dirtyBindings  map[string]struct{}

	outbox         []application.OutboxMessage
	commandsToMark map[string]struct{}
}

func newTransaction(store *Store) *transaction {
	return &transaction{
		store:          store,
		loadedRooms:    make(map[string]*domain.VoiceRoom),
		dirtyRooms:     make(map[string]struct{}),
		loadedBindings: make(map[string]*domain.VoiceChannelBinding),
		dirtyBindings:  make(map[string]struct{}),
		outbox:         make([]application.OutboxMessage, 0),
		commandsToMark: make(map[string]struct{}),
	}
}

func (t *transaction) GetRoom(ctx context.Context, roomID string) (*domain.VoiceRoom, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if room, ok := t.loadedRooms[roomID]; ok {
		return room.Clone(), nil
	}

	room, err := t.store.getRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}

	t.loadedRooms[roomID] = room.Clone()
	return room.Clone(), nil
}

func (t *transaction) SaveRoom(ctx context.Context, room *domain.VoiceRoom) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if room == nil {
		return domain.ErrInvalidIdentifier
	}

	t.loadedRooms[room.ID] = room.Clone()
	t.dirtyRooms[room.ID] = struct{}{}
	return nil
}

func (t *transaction) GetBindingByChannel(ctx context.Context, workspaceID string, channelID string) (*domain.VoiceChannelBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := bindingKey(workspaceID, channelID)
	if binding, ok := t.loadedBindings[key]; ok {
		return binding.Clone(), nil
	}

	binding, err := t.store.getBinding(ctx, workspaceID, channelID)
	if err != nil {
		return nil, err
	}

	t.loadedBindings[key] = binding.Clone()
	return binding.Clone(), nil
}

func (t *transaction) SaveBinding(ctx context.Context, binding *domain.VoiceChannelBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if binding == nil {
		return domain.ErrInvalidIdentifier
	}

	key := bindingKey(binding.WorkspaceID, binding.ChannelID)
	t.loadedBindings[key] = binding.Clone()
	t.dirtyBindings[key] = struct{}{}
	return nil
}

func (t *transaction) AppendOutbox(ctx context.Context, message application.OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if message.EventID == "" || message.EventType == "" {
		return domain.ErrInvalidIdentifier
	}

	copied := message
	if message.Payload != nil {
		copied.Payload = make(map[string]string, len(message.Payload))
		for key, value := range message.Payload {
			copied.Payload[key] = value
		}
	}

	t.outbox = append(t.outbox, copied)
	return nil
}

func (t *transaction) IsCommandProcessed(ctx context.Context, commandID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if _, marked := t.commandsToMark[commandID]; marked {
		return true, nil
	}

	return t.store.isCommandProcessed(ctx, commandID)
}

func (t *transaction) MarkCommandProcessed(ctx context.Context, commandID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if commandID == "" {
		return domain.ErrInvalidIdentifier
	}

	t.commandsToMark[commandID] = struct{}{}
	return nil
}

func (t *transaction) commit(ctx context.Context) error {
	if err := t.commitRooms(ctx); err != nil {
		return err
	}

	if err := t.commitBindings(ctx); err != nil {
		return err
	}

	for _, message := range t.outbox {
		if err := t.store.appendOutbox(ctx, message); err != nil {
			return err
		}
	}

	commandIDs := sortedKeys(t.commandsToMark)
	for _, commandID := range commandIDs {
		if err := t.store.markCommandProcessed(ctx, commandID); err != nil {
			return err
		}
	}

	return nil
}

func (t *transaction) commitRooms(ctx context.Context) error {
	roomIDs := sortedKeys(t.dirtyRooms)
	for _, roomID := range roomIDs {
		room, ok := t.loadedRooms[roomID]
		if !ok {
			continue
		}

		if err := t.store.saveRoom(ctx, room); err != nil {
			return err
		}
	}

	return nil
}

func (t *transaction) commitBindings(ctx context.Context) error {
	bindingIDs := sortedKeys(t.dirtyBindings)
	for _, key := range bindingIDs {
		binding, ok := t.loadedBindings[key]
		if !ok {
			continue
		}

		if err := t.store.saveBinding(ctx, binding); err != nil {
			return err
		}
	}

	return nil
}

type roomSnapshot struct {
	RoomID       string
	WorkspaceID  string
	ChannelID    string
	Active       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Participants []domain.VoiceParticipantSession
}

func rebuildRoom(snapshot roomSnapshot) (*domain.VoiceRoom, error) {
	room, err := domain.NewVoiceRoom(snapshot.RoomID, snapshot.WorkspaceID, snapshot.ChannelID, snapshot.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("create room snapshot: %w", err)
	}

	for _, participant := range snapshot.Participants {
		if err = applyParticipant(room, participant, snapshot.CreatedAt); err != nil {
			return nil, err
		}
	}

	room.Active = snapshot.Active
	room.CreatedAt = snapshot.CreatedAt
	room.UpdatedAt = snapshot.UpdatedAt
	return room, nil
}

func applyParticipant(room *domain.VoiceRoom, participant domain.VoiceParticipantSession, fallback time.Time) error {
	joinedAt := participant.JoinedAt
	if joinedAt.IsZero() {
		joinedAt = fallback
	}

	if _, err := room.JoinParticipant(participant.UserID, joinedAt); err != nil {
		return fmt.Errorf("join participant from snapshot: %w", err)
	}

	if participant.MicrophoneMuted {
		if _, err := room.SetMicrophoneMuted(participant.UserID, true, joinedAt); err != nil {
			return fmt.Errorf("set participant microphone from snapshot: %w", err)
		}
	}

	if participant.CameraEnabled {
		if _, err := room.SetCameraEnabled(participant.UserID, true, joinedAt); err != nil {
			return fmt.Errorf("set participant camera from snapshot: %w", err)
		}
	}

	if participant.LeftAt != nil {
		if _, err := room.LeaveParticipant(participant.UserID, *participant.LeftAt); err != nil {
			return fmt.Errorf("leave participant from snapshot: %w", err)
		}
	}

	return nil
}

func bindingKey(workspaceID string, channelID string) string {
	return workspaceID + ":" + channelID
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys
}

func parseConsistency(raw string) (gocql.Consistency, error) {
	normalized := strings.ToUpper(strings.TrimSpace(raw))
	switch normalized {
	case "ANY":
		return gocql.Any, nil
	case "ONE":
		return gocql.One, nil
	case "TWO":
		return gocql.Two, nil
	case "THREE":
		return gocql.Three, nil
	case "QUORUM":
		return gocql.Quorum, nil
	case "ALL":
		return gocql.All, nil
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum, nil
	case "EACH_QUORUM":
		return gocql.EachQuorum, nil
	case "LOCAL_ONE":
		return gocql.LocalOne, nil
	default:
		return 0, fmt.Errorf("unsupported scylla consistency %q", raw)
	}
}

func normalizeHosts(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}

		result = append(result, trimmed)
	}

	return result
}
