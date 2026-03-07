package scyllarepo

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

type roomSnapshot struct {
	RoomID       string
	WorkspaceID  string
	ChannelID    string
	Active       bool
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Participants []domain.VoiceParticipantSession
}

func rebuildRoom(snapshot roomSnapshot) (*domain.VoiceRoom, error) {
	room, err := domain.NewVoiceRoom(snapshot.RoomID, snapshot.WorkspaceID, snapshot.ChannelID, snapshot.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("create room snapshot: %w", err)
	}

	for i := range snapshot.Participants {
		participant := snapshot.Participants[i]
		if applyErr := applyParticipant(room, participant, snapshot.CreatedAt); applyErr != nil {
			return nil, applyErr
		}
	}

	room.Active = snapshot.Active
	room.CreatedAt = snapshot.CreatedAt
	room.UpdatedAt = snapshot.UpdatedAt

	return room, nil
}

func applyParticipant(room *domain.VoiceRoom, participant domain.VoiceParticipantSession, fallback time.Time) error {
	joinedAt := participant.JoinedAt
	if joinedAt.IsZero() {
		joinedAt = fallback
	}

	if _, err := room.JoinParticipant(participant.UserID, joinedAt); err != nil {
		return fmt.Errorf("join participant from snapshot: %w", err)
	}

	if participant.MicrophoneMuted {
		if _, err := room.SetMicrophoneMuted(participant.UserID, true, joinedAt); err != nil {
			return fmt.Errorf("set participant microphone from snapshot: %w", err)
		}
	}

	if participant.CameraEnabled {
		if _, err := room.SetCameraEnabled(participant.UserID, true, joinedAt); err != nil {
			return fmt.Errorf("set participant camera from snapshot: %w", err)
		}
	}

	if participant.LeftAt != nil {
		if _, err := room.LeaveParticipant(participant.UserID, *participant.LeftAt); err != nil {
			return fmt.Errorf("leave participant from snapshot: %w", err)
		}
	}

	return nil
}

func toRoomRow(room *domain.VoiceRoom) (roomRow, error) {
	if room == nil {
		return roomRow{}, domain.ErrInvalidIdentifier
	}

	participantsJSON, err := json.Marshal(room.Participants())
	if err != nil {
		return roomRow{}, fmt.Errorf("encode room participants: %w", err)
	}

	return roomRow{
		RoomID:           room.ID,
		WorkspaceID:      room.WorkspaceID,
		ChannelID:        room.ChannelID,
		Active:           room.Active,
		CreatedAt:        room.CreatedAt,
		UpdatedAt:        room.UpdatedAt,
		ParticipantsJSON: string(participantsJSON),
	}, nil
}

func toBindingRow(binding *domain.VoiceChannelBinding) bindingRow {
	return bindingRow{
		WorkspaceID: binding.WorkspaceID,
		ChannelID:   binding.ChannelID,
		RoomID:      binding.RoomID,
		BoundAt:     binding.BoundAt,
		UpdatedAt:   binding.UpdatedAt,
	}
}

func toOutboxRow(message application.OutboxMessage) (outboxRow, error) {
	payloadJSON, err := json.Marshal(message.Payload)
	if err != nil {
		return outboxRow{}, fmt.Errorf("encode outbox payload: %w", err)
	}

	return outboxRow{
		EventID:       message.EventID,
		EventType:     message.EventType,
		CommandID:     message.CommandID,
		CorrelationID: message.CorrelationID,
		CausationID:   message.CausationID,
		MessageID:     message.MessageID,
		OccurredAt:    message.OccurredAt,
		WorkspaceID:   message.WorkspaceID,
		ChannelID:     message.ChannelID,
		RoomID:        message.RoomID,
		ActorID:       message.ActorID,
		SchemaVersion: message.SchemaVersion,
		PayloadJSON:   string(payloadJSON),
		Published:     false,
	}, nil
}

func bindingKey(workspaceID string, channelID string) string {
	return workspaceID + ":" + channelID
}

func sortedKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
