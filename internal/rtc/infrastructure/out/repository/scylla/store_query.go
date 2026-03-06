package scyllarepo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/gocql/gocql"

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

	iter := s.rawSession.Query(
		"SELECT event_id FROM "+outboxUnpublishedTable+" WHERE bucket = ? LIMIT ?",
		outboxUnpublishedBucket,
		limit,
	).WithContext(ctx).Iter()

	eventIDs := make([]string, 0, limit)

	var eventID string
	for iter.Scan(&eventID) {
		eventIDs = append(eventIDs, eventID)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate unpublished outbox ids: %w", err)
	}

	messages := make([]application.OutboxMessage, 0, len(eventIDs))
	for i := range eventIDs {
		row, err := s.getOutboxRow(ctx, eventIDs[i])
		if err != nil {
			if errors.Is(err, gocql.ErrNotFound) {
				_ = s.rawSession.Query(
					"DELETE FROM "+outboxUnpublishedTable+" WHERE bucket = ? AND event_id = ?",
					outboxUnpublishedBucket,
					eventIDs[i],
				).WithContext(ctx).Exec()

				continue
			}

			return nil, err
		}

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

	batch := s.rawSession.Batch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query("UPDATE "+outboxTable+" SET published = true WHERE event_id = ?", eventID)
	batch.Query("DELETE FROM "+outboxUnpublishedTable+" WHERE bucket = ? AND event_id = ?", outboxUnpublishedBucket, eventID)

	if err := s.rawSession.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("mark outbox event published: %w", err)
	}

	return nil
}

func (s *Store) getOutboxRow(ctx context.Context, eventID string) (outboxRow, error) {
	var row outboxRow

	err := s.rawSession.Query(
		"SELECT event_id, event_type, command_id, correlation_id, causation_id, message_id, occurred_at, workspace_id, channel_id, room_id, actor_id, schema_version, payload_json FROM "+outboxTable+" WHERE event_id = ?",
		eventID,
	).WithContext(ctx).Scan(
		&row.EventID,
		&row.EventType,
		&row.CommandID,
		&row.CorrelationID,
		&row.CausationID,
		&row.MessageID,
		&row.OccurredAt,
		&row.WorkspaceID,
		&row.ChannelID,
		&row.RoomID,
		&row.ActorID,
		&row.SchemaVersion,
		&row.PayloadJSON,
	)
	if err != nil {
		return outboxRow{}, fmt.Errorf("query outbox row by id: %w", err)
	}

	return row, nil
}
