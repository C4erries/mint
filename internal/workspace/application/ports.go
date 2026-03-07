package application

import (
	"context"

	"github.com/c4erries/mint/internal/workspace/domain"
)

// WriteRepository wraps workspace write model + outbox in one reliability unit.
type WriteRepository interface {
	WithTx(ctx context.Context, fn func(tx WriteTx) error) error
}

type WriteTx interface {
	GetWorkspace(ctx context.Context, workspaceID string) (domain.Workspace, error)
	SaveWorkspace(ctx context.Context, workspace domain.Workspace) error
	GetChannel(ctx context.Context, workspaceID string, channelID string) (domain.Channel, error)
	SaveChannel(ctx context.Context, channel domain.Channel) error
	GetMember(ctx context.Context, workspaceID string, userID string) (domain.Member, error)
	SaveMember(ctx context.Context, member domain.Member) error
	AppendOutbox(ctx context.Context, message OutboxMessage) error
	IsCommandProcessed(ctx context.Context, commandID string) (bool, error)
	MarkCommandProcessed(ctx context.Context, commandID string) error
}

// ReadRepository exposes workspace query projections.
type ReadRepository interface {
	GetWorkspace(ctx context.Context, workspaceID string) (WorkspaceView, error)
	GetChannel(ctx context.Context, workspaceID string, channelID string) (ChannelView, error)
	GetPermissionSnapshot(ctx context.Context, workspaceID string, channelID string) (domain.PermissionSnapshot, error)
}

// OutboxStore is used by outbox relay.
type OutboxStore interface {
	ListUnpublished(ctx context.Context, limit int) ([]OutboxMessage, error)
	MarkPublished(ctx context.Context, eventID string) error
}

// EventPublisher publishes outbox events.
type EventPublisher interface {
	Publish(ctx context.Context, message OutboxMessage) error
}

// PermissionEvaluator resolves policies against snapshot and may be replaced later.
type PermissionEvaluator interface {
	CanJoinVoiceChannel(snapshot domain.PermissionSnapshot, userID string) (bool, error)
}
