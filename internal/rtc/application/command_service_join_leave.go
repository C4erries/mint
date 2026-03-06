package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/c4erries/mint/internal/rtc/domain"
)

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
			if !errors.Is(err, domain.ErrParticipantAlreadyJoined) {
				return fmt.Errorf("join participant: %w", err)
			}
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room: %w", err)
		}

		if err = tx.SaveBinding(ctx, binding); err != nil {
			return fmt.Errorf("save binding: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventVoiceChannelJoined, room.ID, map[string]string{"user_id": command.UserID})
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
			if !errors.Is(err, domain.ErrParticipantNotFound) || !isParticipantAlreadyLeft(room, command.UserID) {
				return fmt.Errorf("leave participant: %w", err)
			}
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventVoiceChannelLeft, room.ID, map[string]string{"user_id": command.UserID})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox voice leave: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark leave command processed: %w", err)
		}

		return nil
	})
}

func (s *CommandService) ensureRoomAndBinding(
	ctx context.Context,
	tx VoiceRoomWriteTx,
	meta CommandMeta,
) (*domain.VoiceRoom, *domain.VoiceChannelBinding, error) {
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

func isParticipantAlreadyLeft(room *domain.VoiceRoom, userID string) bool {
	participant, found := room.Participant(userID)
	return found && participant.LeftAt != nil
}
