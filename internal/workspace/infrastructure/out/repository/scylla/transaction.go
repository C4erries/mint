package scyllarepo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
)

type transaction struct {
	store *Store

	workspaces      map[string]domain.Workspace
	dirtyWorkspaces map[string]struct{}
	channels        map[string]domain.Channel
	dirtyChannels   map[string]struct{}
	members         map[string]domain.Member
	dirtyMembers    map[string]struct{}
	outbox          []application.OutboxMessage
	commandsToMark  map[string]struct{}
}

func newTransaction(store *Store) *transaction {
	return &transaction{
		store:           store,
		workspaces:      make(map[string]domain.Workspace),
		dirtyWorkspaces: make(map[string]struct{}),
		channels:        make(map[string]domain.Channel),
		dirtyChannels:   make(map[string]struct{}),
		members:         make(map[string]domain.Member),
		dirtyMembers:    make(map[string]struct{}),
		outbox:          make([]application.OutboxMessage, 0),
		commandsToMark:  make(map[string]struct{}),
	}
}

func (t *transaction) GetWorkspace(ctx context.Context, workspaceID string) (domain.Workspace, error) {
	if err := ctx.Err(); err != nil {
		return domain.Workspace{}, err
	}

	if workspace, ok := t.workspaces[workspaceID]; ok {
		return workspace, nil
	}

	workspace, err := t.store.getWorkspace(ctx, workspaceID)
	if err != nil {
		return domain.Workspace{}, err
	}

	t.workspaces[workspaceID] = workspace

	return workspace, nil
}

func (t *transaction) SaveWorkspace(ctx context.Context, workspace domain.Workspace) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	t.workspaces[workspace.ID] = workspace
	t.dirtyWorkspaces[workspace.ID] = struct{}{}

	return nil
}

func (t *transaction) GetChannel(ctx context.Context, workspaceID string, channelID string) (domain.Channel, error) {
	if err := ctx.Err(); err != nil {
		return domain.Channel{}, err
	}

	key := channelKey(workspaceID, channelID)
	if channel, ok := t.channels[key]; ok {
		return channel, nil
	}

	channel, err := t.store.getChannel(ctx, workspaceID, channelID)
	if err != nil {
		return domain.Channel{}, err
	}

	t.channels[key] = channel

	return channel, nil
}

func (t *transaction) SaveChannel(ctx context.Context, channel domain.Channel) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := channelKey(channel.WorkspaceID, channel.ID)
	t.channels[key] = channel
	t.dirtyChannels[key] = struct{}{}

	return nil
}

func (t *transaction) GetMember(ctx context.Context, workspaceID string, userID string) (domain.Member, error) {
	if err := ctx.Err(); err != nil {
		return domain.Member{}, err
	}

	key := memberKey(workspaceID, userID)
	if member, ok := t.members[key]; ok {
		return member, nil
	}

	member, err := t.store.getMember(ctx, workspaceID, userID)
	if err != nil {
		return domain.Member{}, err
	}

	t.members[key] = member

	return member, nil
}

func (t *transaction) SaveMember(ctx context.Context, member domain.Member) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	key := memberKey(member.WorkspaceID, member.UserID)
	t.members[key] = member
	t.dirtyMembers[key] = struct{}{}

	return nil
}

func (t *transaction) AppendOutbox(ctx context.Context, message application.OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if message.EventID == "" || message.EventType == "" {
		return domain.ErrInvalidIdentifier
	}

	t.outbox = append(t.outbox, message)

	return nil
}

func (t *transaction) IsCommandProcessed(ctx context.Context, commandID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if _, exists := t.commandsToMark[commandID]; exists {
		return true, nil
	}

	var storedCommandID string

	err := t.store.session.Query(
		"SELECT command_id FROM "+processedCommandsTable+" WHERE command_id = ?",
		commandID,
	).WithContext(ctx).Scan(&storedCommandID)
	if err == nil {
		return true, nil
	}

	if err == gocql.ErrNotFound {
		return false, nil
	}

	return false, fmt.Errorf("query processed command: %w", err)
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
	batch := t.store.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	entries := 0

	for _, key := range sortedKeys(t.dirtyWorkspaces) {
		workspace := t.workspaces[key]
		batch.Query(
			"INSERT INTO "+workspacesTable+" (workspace_id, name, owner_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
			workspace.ID,
			workspace.Name,
			workspace.OwnerID,
			workspace.CreatedAt,
			workspace.UpdatedAt,
		)

		entries++
	}

	for _, key := range sortedKeys(t.dirtyChannels) {
		channel := t.channels[key]
		batch.Query(
			"INSERT INTO "+channelsTable+" (workspace_id, channel_id, name, kind, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
			channel.WorkspaceID,
			channel.ID,
			channel.Name,
			channel.Kind,
			channel.CreatedAt,
			channel.UpdatedAt,
		)

		entries++
	}

	for _, key := range sortedKeys(t.dirtyMembers) {
		member := t.members[key]

		rolesJSON, err := json.Marshal(member.RoleIDs)
		if err != nil {
			return fmt.Errorf("marshal member roles: %w", err)
		}

		batch.Query(
			"INSERT INTO "+membersTable+" (workspace_id, user_id, joined_at, banned, roles_json) VALUES (?, ?, ?, ?, ?)",
			member.WorkspaceID,
			member.UserID,
			member.JoinedAt,
			member.Banned,
			string(rolesJSON),
		)

		entries++
	}

	for _, message := range t.outbox {
		payloadJSON, err := json.Marshal(message.Payload)
		if err != nil {
			return fmt.Errorf("marshal outbox payload: %w", err)
		}

		batch.Query(
			"INSERT INTO "+outboxTable+" (event_id, event_type, command_id, correlation_id, causation_id, message_id, occurred_at, workspace_id, channel_id, actor_id, schema_version, payload_json, published) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			message.EventID,
			message.EventType,
			message.CommandID,
			message.CorrelationID,
			message.CausationID,
			message.MessageID,
			message.OccurredAt,
			message.WorkspaceID,
			message.ChannelID,
			message.ActorID,
			message.SchemaVersion,
			string(payloadJSON),
			false,
		)

		entries++
	}

	processedAt := t.store.now().UTC()
	for _, commandID := range sortedKeys(t.commandsToMark) {
		batch.Query(
			"INSERT INTO "+processedCommandsTable+" (command_id, processed_at) VALUES (?, ?)",
			commandID,
			processedAt,
		)

		entries++
	}

	if entries == 0 {
		return nil
	}

	if err := t.store.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("execute workspace write batch: %w", err)
	}

	return nil
}

func channelKey(workspaceID string, channelID string) string {
	return workspaceID + ":" + channelID
}

func memberKey(workspaceID string, userID string) string {
	return workspaceID + ":" + userID
}
