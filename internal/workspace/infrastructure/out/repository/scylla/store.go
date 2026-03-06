package scyllarepo

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
)

const (
	defaultPort        = 9042
	defaultConsistency = "quorum"
)

const (
	workspacesTable        = "core_workspace_workspaces"
	channelsTable          = "core_workspace_channels"
	membersTable           = "core_workspace_members"
	rolesTable             = "core_workspace_roles"
	overridesTable         = "core_workspace_channel_overrides"
	processedCommandsTable = "core_workspace_processed_commands"
	outboxTable            = "core_workspace_outbox"
)

type SessionFactory interface {
	CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error)
}

type defaultSessionFactory struct{}

func (defaultSessionFactory) CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error) {
	return cluster.CreateSession()
}

type Options struct {
	Hosts            []string
	Port             int
	Keyspace         string
	Consistency      string
	AutoCreateSchema bool
	Now              func() time.Time
	SessionFactory   SessionFactory
}

// Store implements workspace write/read repositories and outbox store.
type Store struct {
	session *gocql.Session
	now     func() time.Time
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
		if err = ensureKeyspace(options, hosts, consistency); err != nil {
			return nil, err
		}
	}

	session, err := createKeyspaceSession(options, hosts, consistency)
	if err != nil {
		return nil, err
	}

	if options.AutoCreateSchema {
		if err = ensureSchema(context.Background(), session); err != nil {
			session.Close()
			return nil, err
		}
	}

	return &Store{session: session, now: options.Now}, nil
}

func (s *Store) Close() error {
	if s == nil || s.session == nil {
		return nil
	}

	s.session.Close()
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.session == nil {
		return fmt.Errorf("scylla session is not initialized")
	}

	var releaseVersion string
	if err := s.session.Query("SELECT release_version FROM system.local LIMIT 1").WithContext(ctx).Scan(&releaseVersion); err != nil {
		return fmt.Errorf("ping scylla: %w", err)
	}

	return nil
}

func (s *Store) WithTx(ctx context.Context, fn func(tx application.WriteTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tx := newTransaction(s)
	if err := fn(tx); err != nil {
		return err
	}

	return tx.commit(ctx)
}

func (s *Store) GetWorkspace(ctx context.Context, workspaceID string) (application.WorkspaceView, error) {
	workspace, err := s.getWorkspace(ctx, workspaceID)
	if err != nil {
		return application.WorkspaceView{}, err
	}

	return application.WorkspaceView{
		ID:        workspace.ID,
		Name:      workspace.Name,
		OwnerID:   workspace.OwnerID,
		CreatedAt: workspace.CreatedAt,
		UpdatedAt: workspace.UpdatedAt,
	}, nil
}

func (s *Store) GetChannel(ctx context.Context, workspaceID string, channelID string) (application.ChannelView, error) {
	channel, err := s.getChannel(ctx, workspaceID, channelID)
	if err != nil {
		return application.ChannelView{}, err
	}

	return application.ChannelView{
		WorkspaceID: channel.WorkspaceID,
		ID:          channel.ID,
		Name:        channel.Name,
		Kind:        channel.Kind,
		CreatedAt:   channel.CreatedAt,
		UpdatedAt:   channel.UpdatedAt,
	}, nil
}

func (s *Store) GetPermissionSnapshot(ctx context.Context, workspaceID string, channelID string) (domain.PermissionSnapshot, error) {
	if _, err := s.getWorkspace(ctx, workspaceID); err != nil {
		return domain.PermissionSnapshot{}, err
	}

	if _, err := s.getChannel(ctx, workspaceID, channelID); err != nil {
		return domain.PermissionSnapshot{}, err
	}

	members, err := s.listMembers(ctx, workspaceID)
	if err != nil {
		return domain.PermissionSnapshot{}, err
	}

	roles, err := s.listRoles(ctx, workspaceID)
	if err != nil {
		return domain.PermissionSnapshot{}, err
	}

	overrides, err := s.listOverrides(ctx, workspaceID, channelID)
	if err != nil {
		return domain.PermissionSnapshot{}, err
	}

	memberMap := make(map[string]domain.Member, len(members))
	for _, member := range members {
		memberMap[member.UserID] = member
	}

	roleMap := make(map[string]domain.Role, len(roles))
	for _, role := range roles {
		roleMap[role.ID] = role
	}

	return domain.PermissionSnapshot{
		WorkspaceID:      workspaceID,
		ChannelID:        channelID,
		Members:          memberMap,
		RolesByID:        roleMap,
		ChannelOverrides: overrides,
	}, nil
}

func (s *Store) ListUnpublished(ctx context.Context, limit int) ([]application.OutboxMessage, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("outbox limit must be > 0")
	}

	iter := s.session.Query(
		"SELECT event_id, event_type, command_id, correlation_id, causation_id, message_id, occurred_at, workspace_id, channel_id, actor_id, schema_version, payload_json FROM "+outboxTable+" WHERE published = false LIMIT ? ALLOW FILTERING",
		limit,
	).WithContext(ctx).Iter()

	messages := make([]application.OutboxMessage, 0)

	var (
		eventID       string
		eventType     string
		commandID     string
		correlationID string
		causationID   string
		messageID     string
		occurredAt    time.Time
		workspaceID   string
		channelID     string
		actorID       string
		schemaVersion int
		payloadJSON   string
	)

	for iter.Scan(
		&eventID,
		&eventType,
		&commandID,
		&correlationID,
		&causationID,
		&messageID,
		&occurredAt,
		&workspaceID,
		&channelID,
		&actorID,
		&schemaVersion,
		&payloadJSON,
	) {
		payload := make(map[string]string)
		if payloadJSON != "" {
			if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
				_ = iter.Close()
				return nil, fmt.Errorf("decode workspace outbox payload: %w", err)
			}
		}

		messages = append(messages, application.OutboxMessage{
			EventID:       eventID,
			EventType:     eventType,
			CommandID:     commandID,
			CorrelationID: correlationID,
			CausationID:   causationID,
			MessageID:     messageID,
			OccurredAt:    occurredAt,
			WorkspaceID:   workspaceID,
			ChannelID:     channelID,
			ActorID:       actorID,
			SchemaVersion: schemaVersion,
			Payload:       payload,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate workspace outbox: %w", err)
	}

	return messages, nil
}

func (s *Store) MarkPublished(ctx context.Context, eventID string) error {
	if eventID == "" {
		return fmt.Errorf("event id is required")
	}

	if err := s.session.Query(
		"UPDATE "+outboxTable+" SET published = true WHERE event_id = ?",
		eventID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("mark workspace outbox event published: %w", err)
	}

	return nil
}

func (s *Store) getWorkspace(ctx context.Context, workspaceID string) (domain.Workspace, error) {
	var workspace domain.Workspace
	if err := s.session.Query(
		"SELECT workspace_id, name, owner_id, created_at, updated_at FROM "+workspacesTable+" WHERE workspace_id = ?",
		workspaceID,
	).WithContext(ctx).Scan(&workspace.ID, &workspace.Name, &workspace.OwnerID, &workspace.CreatedAt, &workspace.UpdatedAt); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Workspace{}, domain.ErrWorkspaceNotFound
		}

		return domain.Workspace{}, fmt.Errorf("query workspace by id: %w", err)
	}

	return workspace, nil
}

func (s *Store) getChannel(ctx context.Context, workspaceID string, channelID string) (domain.Channel, error) {
	var channel domain.Channel
	if err := s.session.Query(
		"SELECT workspace_id, channel_id, name, kind, created_at, updated_at FROM "+channelsTable+" WHERE workspace_id = ? AND channel_id = ?",
		workspaceID,
		channelID,
	).WithContext(ctx).Scan(
		&channel.WorkspaceID,
		&channel.ID,
		&channel.Name,
		&channel.Kind,
		&channel.CreatedAt,
		&channel.UpdatedAt,
	); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Channel{}, domain.ErrChannelNotFound
		}

		return domain.Channel{}, fmt.Errorf("query channel by id: %w", err)
	}

	return channel, nil
}

func (s *Store) getMember(ctx context.Context, workspaceID string, userID string) (domain.Member, error) {
	var rolesJSON string
	member := domain.Member{}
	if err := s.session.Query(
		"SELECT workspace_id, user_id, joined_at, banned, roles_json FROM "+membersTable+" WHERE workspace_id = ? AND user_id = ?",
		workspaceID,
		userID,
	).WithContext(ctx).Scan(&member.WorkspaceID, &member.UserID, &member.JoinedAt, &member.Banned, &rolesJSON); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Member{}, domain.ErrMemberNotFound
		}

		return domain.Member{}, fmt.Errorf("query member by id: %w", err)
	}

	if rolesJSON != "" {
		if err := json.Unmarshal([]byte(rolesJSON), &member.RoleIDs); err != nil {
			return domain.Member{}, fmt.Errorf("decode member roles: %w", err)
		}
	} else {
		member.RoleIDs = make([]string, 0)
	}

	return member, nil
}

func (s *Store) listMembers(ctx context.Context, workspaceID string) ([]domain.Member, error) {
	iter := s.session.Query(
		"SELECT workspace_id, user_id, joined_at, banned, roles_json FROM "+membersTable+" WHERE workspace_id = ?",
		workspaceID,
	).WithContext(ctx).Iter()

	members := make([]domain.Member, 0)

	var (
		member    domain.Member
		rolesJSON string
	)

	for iter.Scan(&member.WorkspaceID, &member.UserID, &member.JoinedAt, &member.Banned, &rolesJSON) {
		current := member
		if rolesJSON != "" {
			if err := json.Unmarshal([]byte(rolesJSON), &current.RoleIDs); err != nil {
				_ = iter.Close()
				return nil, fmt.Errorf("decode member roles: %w", err)
			}
		} else {
			current.RoleIDs = make([]string, 0)
		}

		members = append(members, current)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate members: %w", err)
	}

	return members, nil
}

func (s *Store) listRoles(ctx context.Context, workspaceID string) ([]domain.Role, error) {
	iter := s.session.Query(
		"SELECT workspace_id, role_id, name, permissions_json, created_at, updated_at FROM "+rolesTable+" WHERE workspace_id = ?",
		workspaceID,
	).WithContext(ctx).Iter()

	roles := make([]domain.Role, 0)

	var (
		role            domain.Role
		permissionsJSON string
	)

	for iter.Scan(&role.WorkspaceID, &role.ID, &role.Name, &permissionsJSON, &role.CreatedAt, &role.UpdatedAt) {
		current := role
		if permissionsJSON != "" {
			if err := json.Unmarshal([]byte(permissionsJSON), &current.Permissions); err != nil {
				_ = iter.Close()
				return nil, fmt.Errorf("decode role permissions: %w", err)
			}
		} else {
			current.Permissions = make([]string, 0)
		}

		roles = append(roles, current)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate roles: %w", err)
	}

	return roles, nil
}

func (s *Store) listOverrides(ctx context.Context, workspaceID string, channelID string) ([]domain.ChannelOverride, error) {
	iter := s.session.Query(
		"SELECT workspace_id, channel_id, subject_type, subject_id, allow_mask, deny_mask, updated_at FROM "+overridesTable+" WHERE workspace_id = ? AND channel_id = ?",
		workspaceID,
		channelID,
	).WithContext(ctx).Iter()

	overrides := make([]domain.ChannelOverride, 0)
	var override domain.ChannelOverride
	for iter.Scan(&override.WorkspaceID, &override.ChannelID, &override.SubjectType, &override.SubjectID, &override.AllowMask, &override.DenyMask, &override.UpdatedAt) {
		overrides = append(overrides, override)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate channel overrides: %w", err)
	}

	return overrides, nil
}

func sortedKeys[T any](value map[string]T) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}
