package application

import (
	"context"
	"fmt"

	"github.com/c4erries/mint/internal/rtc/domain"
)

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

		message := s.newOutboxMessage(command.Meta, domain.EventVoiceChannelLeft, room.ID, map[string]string{"reason": "session_terminated"})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox terminate: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark terminate command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) updateMicrophoneState(
	ctx context.Context,
	meta CommandMeta,
	userID string,
	muted bool,
	eventType string,
) error {
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

func (s *CommandService) updateCameraState(
	ctx context.Context,
	meta CommandMeta,
	userID string,
	enabled bool,
	eventType string,
) error {
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
