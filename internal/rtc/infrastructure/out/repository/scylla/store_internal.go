package scyllarepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/c4erries/mint/internal/rtc/domain"
)

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

	_, err := s.session.Query(stmt, names).
		BindMap(qb.M{
			"command_id":   commandID,
			"processed_at": s.now().UTC(),
		}).
		WithContext(ctx).
		ExecCASRelease()
	if err != nil {
		return fmt.Errorf("mark command processed: %w", err)
	}

	return nil
}
