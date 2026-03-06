package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/c4erries/mint/internal/rtc/domain"
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

func (s *CommandService) JoinVoiceChannel(ctx context.Context, command JoinVoiceChannelCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	if err := s.ensureJoinPermission(ctx, command.Meta.WorkspaceID, command.Meta.ChannelID, command.UserID); err != nil {
		return err
	}

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed join command: %w", err)
		}

		if processed {
			return nil
		}

		room, binding, err := s.ensureRoomAndBinding(ctx, tx, command.Meta)
		if err != nil {
			return err
		}

		if _, err = room.JoinParticipant(command.UserID, command.Meta.OccurredAt); err != nil {
			return fmt.Errorf("join participant: %w", err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room: %w", err)
		}

		if err = tx.SaveBinding(ctx, binding); err != nil {
			return fmt.Errorf("save binding: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventVoiceChannelJoined, room.ID, map[string]string{
			"user_id": command.UserID,
		})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox voice join: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark join command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) LeaveVoiceChannel(ctx context.Context, command LeaveVoiceChannelCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed leave command: %w", err)
		}

		if processed {
			return nil
		}

		room, err := s.loadRoom(ctx, tx, command.Meta)
		if err != nil {
			return err
		}

		if _, err = room.LeaveParticipant(command.UserID, command.Meta.OccurredAt); err != nil {
			return fmt.Errorf("leave participant: %w", err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventVoiceChannelLeft, room.ID, map[string]string{
			"user_id": command.UserID,
		})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox voice leave: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark leave command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) MuteSelf(ctx context.Context, command MuteSelfCommand) error {
	return s.updateMicrophoneState(ctx, command.Meta, command.UserID, true, domain.EventMicrophoneMuted)
}

func (s *CommandService) UnmuteSelf(ctx context.Context, command UnmuteSelfCommand) error {
	return s.updateMicrophoneState(ctx, command.Meta, command.UserID, false, domain.EventMicrophoneUnmuted)
}

func (s *CommandService) EnableCamera(ctx context.Context, command EnableCameraCommand) error {
	return s.updateCameraState(ctx, command.Meta, command.UserID, true, domain.EventCameraEnabled)
}

func (s *CommandService) DisableCamera(ctx context.Context, command DisableCameraCommand) error {
	return s.updateCameraState(ctx, command.Meta, command.UserID, false, domain.EventCameraDisabled)
}

func (s *CommandService) TerminateVoiceSession(ctx context.Context, command TerminateVoiceSessionCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed terminate command: %w", err)
		}

		if processed {
			return nil
		}

		room, err := s.loadRoom(ctx, tx, command.Meta)
		if err != nil {
			return err
		}

		if err = room.Terminate(command.Meta.OccurredAt); err != nil {
			return fmt.Errorf("terminate room: %w", err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventVoiceChannelLeft, room.ID, map[string]string{
			"reason": "session_terminated",
		})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox terminate: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark terminate command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) IssueRtcToken(ctx context.Context, command IssueRtcTokenCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	if err := s.ensureJoinPermission(ctx, command.Meta.WorkspaceID, command.Meta.ChannelID, command.UserID); err != nil {
		return err
	}

	ttl := command.TTL
	if ttl == 0 {
		ttl = s.defaultTokenTTL
	}

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed issue token command: %w", err)
		}

		if processed {
			return nil
		}

		room, err := s.loadRoom(ctx, tx, command.Meta)
		if err != nil {
			return err
		}

		participant, found := room.Participant(command.UserID)
		if !found || participant.LeftAt != nil {
			return domain.ErrParticipantNotFound
		}

		issuedToken, err := s.livekit.IssueToken(ctx, LiveKitTokenRequest{
			RoomID:       room.ID,
			UserID:       command.UserID,
			TTL:          ttl,
			CanPublish:   command.CanPublish,
			CanSubscribe: command.CanSubscribe,
		})
		if err != nil {
			return fmt.Errorf("issue livekit token: %w", err)
		}

		issuedAt := issuedToken.IssuedAt
		if issuedAt.IsZero() {
			issuedAt = s.now()
		}

		grant, err := domain.NewMediaAccessGrant(
			issuedToken.TokenID,
			command.Meta.CommandID,
			room.ID,
			command.UserID,
			issuedToken.Token,
			command.CanPublish,
			command.CanSubscribe,
			issuedAt,
			issuedToken.ExpiresAt,
		)
		if err != nil {
			return fmt.Errorf("create grant: %w", err)
		}

		if err = s.grants.SaveGrant(ctx, grant); err != nil {
			return fmt.Errorf("save grant: %w", err)
		}

		if err = room.Touch(command.Meta.OccurredAt); err != nil {
			return fmt.Errorf("touch room state: %w", err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room after token issue: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventRtcTokenIssued, room.ID, map[string]string{
			"token_id":    issuedToken.TokenID,
			"user_id":     command.UserID,
			"expires_at":  issuedToken.ExpiresAt.Format(time.RFC3339Nano),
			"ttl_seconds": strconv.FormatInt(int64(ttl.Seconds()), 10),
		})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox token issue: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark issue token command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) updateMicrophoneState(ctx context.Context, meta CommandMeta, userID string, muted bool, eventType string) error {
	if err := meta.Validate(); err != nil || userID == "" {
		return ErrInvalidCommand
	}

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed microphone command: %w", err)
		}

		if processed {
			return nil
		}

		room, err := s.loadRoom(ctx, tx, meta)
		if err != nil {
			return err
		}

		if _, err = room.SetMicrophoneMuted(userID, muted, meta.OccurredAt); err != nil {
			return fmt.Errorf("set microphone muted=%t: %w", muted, err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room after microphone change: %w", err)
		}

		message := s.newOutboxMessage(meta, eventType, room.ID, map[string]string{"user_id": userID})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox microphone change: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, meta.CommandID); err != nil {
			return fmt.Errorf("mark microphone command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) updateCameraState(ctx context.Context, meta CommandMeta, userID string, enabled bool, eventType string) error {
	if err := meta.Validate(); err != nil || userID == "" {
		return ErrInvalidCommand
	}

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed camera command: %w", err)
		}

		if processed {
			return nil
		}

		room, err := s.loadRoom(ctx, tx, meta)
		if err != nil {
			return err
		}

		if _, err = room.SetCameraEnabled(userID, enabled, meta.OccurredAt); err != nil {
			return fmt.Errorf("set camera enabled=%t: %w", enabled, err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room after camera change: %w", err)
		}

		message := s.newOutboxMessage(meta, eventType, room.ID, map[string]string{"user_id": userID})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox camera change: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, meta.CommandID); err != nil {
			return fmt.Errorf("mark camera command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) ensureJoinPermission(ctx context.Context, workspaceID string, channelID string, userID string) error {
	allowed, err := s.permissions.CanJoinVoiceChannel(ctx, workspaceID, channelID, userID)
	if err != nil {
		return fmt.Errorf("permission check failed: %w", err)
	}

	if !allowed {
		return ErrPermissionDenied
	}

	return nil
}

func (s *CommandService) ensureRoomAndBinding(ctx context.Context, tx VoiceRoomWriteTx, meta CommandMeta) (*domain.VoiceRoom, *domain.VoiceChannelBinding, error) {
	binding, err := tx.GetBindingByChannel(ctx, meta.WorkspaceID, meta.ChannelID)
	if err != nil {
		if !errors.Is(err, domain.ErrVoiceChannelBindingNotFound) {
			return nil, nil, fmt.Errorf("load binding: %w", err)
		}

		return s.createRoomAndBinding(meta)
	}

	room, err := tx.GetRoom(ctx, binding.RoomID)
	if err != nil {
		if !errors.Is(err, domain.ErrVoiceRoomNotFound) {
			return nil, nil, fmt.Errorf("load room by binding: %w", err)
		}

		newRoom, newBinding, createErr := s.createRoomAndBinding(meta)
		if createErr != nil {
			return nil, nil, createErr
		}

		if err = newBinding.Rebind(newRoom.ID, meta.OccurredAt); err != nil {
			return nil, nil, fmt.Errorf("rebind room after missing room: %w", err)
		}

		return newRoom, newBinding, nil
	}

	if room.Active {
		return room, binding, nil
	}

	newRoom, _, err := s.createRoomAndBinding(meta)
	if err != nil {
		return nil, nil, err
	}

	if err = binding.Rebind(newRoom.ID, meta.OccurredAt); err != nil {
		return nil, nil, fmt.Errorf("rebind inactive room: %w", err)
	}

	return newRoom, binding, nil
}

func (s *CommandService) loadRoom(ctx context.Context, tx VoiceRoomWriteTx, meta CommandMeta) (*domain.VoiceRoom, error) {
	if meta.RoomID != "" {
		room, err := tx.GetRoom(ctx, meta.RoomID)
		if err == nil {
			return room, nil
		}

		if !errors.Is(err, domain.ErrVoiceRoomNotFound) {
			return nil, fmt.Errorf("load room by id: %w", err)
		}
	}

	binding, err := tx.GetBindingByChannel(ctx, meta.WorkspaceID, meta.ChannelID)
	if err != nil {
		return nil, fmt.Errorf("load binding by channel: %w", err)
	}

	room, err := tx.GetRoom(ctx, binding.RoomID)
	if err != nil {
		return nil, fmt.Errorf("load room from binding: %w", err)
	}

	return room, nil
}

func (s *CommandService) createRoomAndBinding(meta CommandMeta) (*domain.VoiceRoom, *domain.VoiceChannelBinding, error) {
	roomID := meta.RoomID
	if roomID == "" {
		roomID = s.idGenerator()
	}

	room, err := domain.NewVoiceRoom(roomID, meta.WorkspaceID, meta.ChannelID, meta.OccurredAt)
	if err != nil {
		return nil, nil, fmt.Errorf("create voice room: %w", err)
	}

	binding, err := domain.NewVoiceChannelBinding(meta.WorkspaceID, meta.ChannelID, roomID, meta.OccurredAt)
	if err != nil {
		return nil, nil, fmt.Errorf("create voice channel binding: %w", err)
	}

	return room, binding, nil
}

func (s *CommandService) newOutboxMessage(meta CommandMeta, eventType string, roomID string, payload map[string]string) OutboxMessage {
	messageID := meta.MessageID
	if messageID == "" {
		messageID = s.idGenerator()
	}

	return OutboxMessage{
		EventID:       s.idGenerator(),
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
