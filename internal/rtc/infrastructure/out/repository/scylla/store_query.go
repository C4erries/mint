package scyllarepo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

func (s *Store) GetVoiceRoomState(ctx context.Context, workspaceID string, channelID string) (application.VoiceRoomState, error) {
	binding, err := s.getBinding(ctx, workspaceID, channelID)
	if err != nil {
		return application.VoiceRoomState{}, err
	}

	room, err := s.getRoom(ctx, binding.RoomID)
	if err != nil {
		return application.VoiceRoomState{}, err
	}

	return application.VoiceRoomState{
		RoomID:            room.ID,
		WorkspaceID:       room.WorkspaceID,
		ChannelID:         room.ChannelID,
		Active:            room.Active,
		ParticipantCount:  room.ActiveParticipantCount(),
		LastStateChangeAt: room.UpdatedAt,
	}, nil
}

func (s *Store) ListVoiceParticipants(ctx context.Context, roomID string) ([]application.VoiceParticipant, error) {
	room, err := s.getRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}

	participants := room.Participants()
	result := make([]application.VoiceParticipant, 0, len(participants))

	for i := range participants {
		participant := participants[i]
		result = append(result, application.VoiceParticipant{
			UserID:          participant.UserID,
			JoinedAt:        participant.JoinedAt,
			LeftAt:          participant.LeftAt,
			MicrophoneMuted: participant.MicrophoneMuted,
			CameraEnabled:   participant.CameraEnabled,
		})
	}

	return result, nil
}

func (s *Store) GetVoiceChannelBinding(ctx context.Context, workspaceID string, channelID string) (application.VoiceChannelBindingView, error) {
	binding, err := s.getBinding(ctx, workspaceID, channelID)
	if err != nil {
		return application.VoiceChannelBindingView{}, err
	}

	return application.VoiceChannelBindingView{
		WorkspaceID: binding.WorkspaceID,
		ChannelID:   binding.ChannelID,
		RoomID:      binding.RoomID,
		UpdatedAt:   binding.UpdatedAt,
	}, nil
}

func (s *Store) ListUnpublished(ctx context.Context, limit int) ([]application.OutboxMessage, error) {
	if limit <= 0 {
		limit = 100
	}

	stmt, names := qb.Select(outboxTable).
		Columns(
			"event_id",
			"event_type",
			"command_id",
			"correlation_id",
			"causation_id",
			"message_id",
			"occurred_at",
			"workspace_id",
			"channel_id",
			"room_id",
			"actor_id",
			"schema_version",
			"payload_json",
		).
		Where(qb.Eq("published")).
		Limit(uint(limit)).
		AllowFiltering().
		ToCql()

	rows := make([]outboxRow, 0, limit)

	err := s.session.Query(stmt, names).
		BindMap(qb.M{"published": false}).
		WithContext(ctx).
		SelectRelease(&rows)
	if err != nil {
		return nil, fmt.Errorf("select unpublished outbox rows: %w", err)
	}

	messages := make([]application.OutboxMessage, 0, len(rows))
	for i := range rows {
		row := rows[i]

		payload := map[string]string{}
		if row.PayloadJSON != "" {
			if unmarshalErr := json.Unmarshal([]byte(row.PayloadJSON), &payload); unmarshalErr != nil {
				return nil, fmt.Errorf("decode outbox payload: %w", unmarshalErr)
			}
		}

		messages = append(messages, application.OutboxMessage{
			EventID:       row.EventID,
			EventType:     row.EventType,
			CommandID:     row.CommandID,
			CorrelationID: row.CorrelationID,
			CausationID:   row.CausationID,
			MessageID:     row.MessageID,
			OccurredAt:    row.OccurredAt,
			WorkspaceID:   row.WorkspaceID,
			ChannelID:     row.ChannelID,
			RoomID:        row.RoomID,
			ActorID:       row.ActorID,
			SchemaVersion: row.SchemaVersion,
			Payload:       payload,
		})
	}

	return messages, nil
}

func (s *Store) MarkPublished(ctx context.Context, eventID string) error {
	if eventID == "" {
		return domain.ErrInvalidIdentifier
	}

	stmt, names := qb.Update(outboxTable).
		Set("published").
		Where(qb.Eq("event_id")).
		ToCql()

	if err := s.session.Query(stmt, names).
		BindMap(qb.M{"event_id": eventID, "published": true}).
		WithContext(ctx).
		ExecRelease(); err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}

	return nil
}
