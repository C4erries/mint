package application

import (
	"context"
	"time"

	"github.com/c4erries/mint/internal/rtc/domain"
)

// VoiceRoomWriteRepository coordinates write model and outbox in one unit of reliability.
type VoiceRoomWriteRepository interface {
	WithTx(ctx context.Context, fn func(tx VoiceRoomWriteTx) error) error
}

type VoiceRoomWriteTx interface {
	GetRoom(ctx context.Context, roomID string) (*domain.VoiceRoom, error)
	SaveRoom(ctx context.Context, room *domain.VoiceRoom) error
	GetBindingByChannel(ctx context.Context, workspaceID string, channelID string) (*domain.VoiceChannelBinding, error)
	SaveBinding(ctx context.Context, binding *domain.VoiceChannelBinding) error
	AppendOutbox(ctx context.Context, message OutboxMessage) error
	IsCommandProcessed(ctx context.Context, commandID string) (bool, error)
	MarkCommandProcessed(ctx context.Context, commandID string) error
}

// VoiceRoomReadRepository exposes optimized query projections.
type VoiceRoomReadRepository interface {
	GetVoiceRoomState(ctx context.Context, workspaceID string, channelID string) (VoiceRoomState, error)
	ListVoiceParticipants(ctx context.Context, roomID string) ([]VoiceParticipant, error)
	GetVoiceChannelBinding(ctx context.Context, workspaceID string, channelID string) (VoiceChannelBindingView, error)
}

// OutboxStore provides polling and ack for outbox relay.
type OutboxStore interface {
	ListUnpublished(ctx context.Context, limit int) ([]OutboxMessage, error)
	MarkPublished(ctx context.Context, eventID string) error
}

// EventPublisher publishes normalized outbox events into transport.
type EventPublisher interface {
	Publish(ctx context.Context, message OutboxMessage) error
}

// PermissionChecker is used for policy checks before applying write commands.
type PermissionChecker interface {
	CanJoinVoiceChannel(ctx context.Context, workspaceID string, channelID string, userID string) (bool, error)
}

// LiveKitTokenRequest is the app-level contract for media token generation.
type LiveKitTokenRequest struct {
	RoomID       string
	UserID       string
	TTL          time.Duration
	CanPublish   bool
	CanSubscribe bool
}

// LiveKitIssuedToken is app-level token contract independent from concrete SDK DTOs.
type LiveKitIssuedToken struct {
	TokenID   string
	Token     string
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// LiveKitClient abstracts token issuance/lifecycle operations.
type LiveKitClient interface {
	IssueToken(ctx context.Context, request LiveKitTokenRequest) (LiveKitIssuedToken, error)
}

// MediaAccessGrantRepository stores short-lived grants (Redis-like behavior with TTL).
type MediaAccessGrantRepository interface {
	SaveGrant(ctx context.Context, grant domain.MediaAccessGrant) error
	GetGrant(ctx context.Context, tokenID string) (domain.MediaAccessGrant, error)
}
