package scyllarepo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/table"

	"github.com/c4erries/mint/internal/rtc/application"
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
	outboxUnpublishedTable = "rtc_outbox_unpublished"
)

const (
	outboxUnpublishedBucket = 0

	insertRoomCQL              = "INSERT INTO rtc_voice_rooms (room_id, workspace_id, channel_id, active, created_at, updated_at, participants_json) VALUES (?, ?, ?, ?, ?, ?, ?)"
	insertBindingCQL           = "INSERT INTO rtc_voice_channel_bindings (workspace_id, channel_id, room_id, bound_at, updated_at) VALUES (?, ?, ?, ?, ?)"
	insertOutboxCQL            = "INSERT INTO rtc_outbox (event_id, event_type, command_id, correlation_id, causation_id, message_id, occurred_at, workspace_id, channel_id, room_id, actor_id, schema_version, payload_json, published) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	insertOutboxUnpublishedCQL = "INSERT INTO rtc_outbox_unpublished (bucket, event_id) VALUES (?, ?)"
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

func (s *Store) Close() error {
	if s == nil || s.rawSession == nil {
		return nil
	}

	s.rawSession.Close()

	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.rawSession == nil {
		return fmt.Errorf("scylla session is not initialized")
	}

	var releaseVersion string
	if err := s.rawSession.Query("SELECT release_version FROM system.local LIMIT 1").WithContext(ctx).Scan(&releaseVersion); err != nil {
		return fmt.Errorf("ping scylla: %w", err)
	}

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
