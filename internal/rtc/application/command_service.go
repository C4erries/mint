package application

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CommandServiceOptions configures write-side orchestration behavior.
type CommandServiceOptions struct {
	DefaultTokenTTL time.Duration
	Now             func() time.Time
	IDGenerator     func() string
}

// CommandService orchestrates write-side use cases.
type CommandService struct {
	rooms           VoiceRoomWriteRepository
	grants          MediaAccessGrantRepository
	permissions     PermissionChecker
	livekit         LiveKitClient
	now             func() time.Time
	idGenerator     func() string
	defaultTokenTTL time.Duration
}

func NewCommandService(
	rooms VoiceRoomWriteRepository,
	grants MediaAccessGrantRepository,
	permissions PermissionChecker,
	livekit LiveKitClient,
	options CommandServiceOptions,
) (*CommandService, error) {
	if options.DefaultTokenTTL <= 0 {
		return nil, fmt.Errorf("default token ttl must be > 0: %w", ErrInvalidCommand)
	}

	if options.Now == nil {
		options.Now = time.Now
	}

	if options.IDGenerator == nil {
		options.IDGenerator = uuid.NewString
	}

	return &CommandService{
		rooms:           rooms,
		grants:          grants,
		permissions:     permissions,
		livekit:         livekit,
		now:             options.Now,
		idGenerator:     options.IDGenerator,
		defaultTokenTTL: options.DefaultTokenTTL,
	}, nil
}

func (s *CommandService) ensureJoinPermission(ctx context.Context, workspaceID, channelID, userID string) error {
	allowed, err := s.permissions.CanJoinVoiceChannel(ctx, workspaceID, channelID, userID)
	if err != nil {
		return fmt.Errorf("permission check failed: %w", err)
	}

	if !allowed {
		return ErrPermissionDenied
	}

	return nil
}

func (s *CommandService) newOutboxMessage(meta CommandMeta, eventType, roomID string, payload map[string]string) OutboxMessage {
	messageID := meta.MessageID
	if messageID == "" {
		messageID = s.idGenerator()
	}

	return OutboxMessage{
		EventID:       s.newEventID(meta.CommandID, eventType),
		EventType:     eventType,
		CommandID:     meta.CommandID,
		CorrelationID: meta.CorrelationID,
		CausationID:   meta.CausationID,
		MessageID:     messageID,
		OccurredAt:    meta.OccurredAt,
		WorkspaceID:   meta.WorkspaceID,
		ChannelID:     meta.ChannelID,
		RoomID:        roomID,
		ActorID:       meta.ActorID,
		SchemaVersion: meta.SchemaVersion,
		Payload:       payload,
	}
}

func (s *CommandService) newEventID(commandID, eventType string) string {
	if commandID == "" {
		return s.idGenerator()
	}

	return commandID + ":" + eventType
}
