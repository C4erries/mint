package livekitwebhook

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"

	"github.com/c4erries/mint/internal/rtc/application"
)

func (h *Handler) validateEvent(event *livekit.WebhookEvent) error {
	if event == nil {
		return application.ErrInvalidCommand
	}

	if strings.TrimSpace(event.GetEvent()) == "" {
		return application.ErrInvalidCommand
	}

	if event.GetRoom() == nil {
		return application.ErrInvalidCommand
	}

	roomID := strings.TrimSpace(event.GetRoom().GetName())
	if roomID == "" {
		roomID = strings.TrimSpace(event.GetRoom().GetSid())
	}

	if roomID == "" {
		return application.ErrInvalidCommand
	}

	workspaceID, channelID, err := roomScope(event)
	if err != nil {
		return err
	}

	if workspaceID == "" || channelID == "" {
		return application.ErrInvalidCommand
	}

	return nil
}

func (h *Handler) handleEvent(ctx context.Context, event *livekit.WebhookEvent) error {
	workspaceID, channelID, err := roomScope(event)
	if err != nil {
		return err
	}

	roomID := strings.TrimSpace(event.GetRoom().GetName())
	if roomID == "" {
		roomID = strings.TrimSpace(event.GetRoom().GetSid())
	}

	userID := participantIdentity(event)
	if userID == "" {
		userID = fallbackActorID
	}

	occurredAt := h.now().UTC()
	if event.GetCreatedAt() > 0 {
		occurredAt = time.Unix(event.GetCreatedAt(), 0).UTC()
	}

	commandID := event.GetId()
	if commandID == "" {
		commandID = h.idGenerator()
	}

	meta := application.CommandMeta{
		CommandID:     commandID,
		CorrelationID: commandID,
		CausationID:   event.GetId(),
		MessageID:     commandID,
		OccurredAt:    occurredAt,
		WorkspaceID:   workspaceID,
		ChannelID:     channelID,
		RoomID:        roomID,
		ActorID:       userID,
		SchemaVersion: 1,
	}

	switch event.GetEvent() {
	case webhook.EventParticipantJoined:
		if userID == fallbackActorID {
			return application.ErrInvalidCommand
		}

		return h.commands.JoinVoiceChannel(ctx, application.JoinVoiceChannelCommand{Meta: meta, UserID: userID})
	case webhook.EventParticipantLeft, webhook.EventParticipantConnectionAborted:
		if userID == fallbackActorID {
			return application.ErrInvalidCommand
		}

		return h.commands.LeaveVoiceChannel(ctx, application.LeaveVoiceChannelCommand{Meta: meta, UserID: userID})
	case webhook.EventRoomFinished:
		return h.commands.TerminateVoiceSession(ctx, application.TerminateVoiceSessionCommand{Meta: meta})
	default:
		h.logger.Debug("skip unsupported livekit callback", slog.String("event_type", event.GetEvent()))
		return nil
	}
}

func roomScope(event *livekit.WebhookEvent) (string, string, error) {
	metadata := strings.TrimSpace(event.GetRoom().GetMetadata())
	if metadata != "" {
		var parsed roomMetadata

		decoder := json.NewDecoder(strings.NewReader(metadata))
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&parsed); err != nil {
			return "", "", fmt.Errorf("decode room metadata: %w", err)
		}

		if parsed.WorkspaceID != "" && parsed.ChannelID != "" {
			return parsed.WorkspaceID, parsed.ChannelID, nil
		}
	}

	participant := event.GetParticipant()
	if participant != nil {
		workspaceID := strings.TrimSpace(participant.GetAttributes()["workspace_id"])

		channelID := strings.TrimSpace(participant.GetAttributes()["channel_id"])
		if workspaceID != "" && channelID != "" {
			return workspaceID, channelID, nil
		}
	}

	return "", "", application.ErrInvalidCommand
}

func participantIdentity(event *livekit.WebhookEvent) string {
	if event == nil || event.GetParticipant() == nil {
		return ""
	}

	return strings.TrimSpace(event.GetParticipant().GetIdentity())
}

type roomMetadata struct {
	WorkspaceID string `json:"workspace_id"`
	ChannelID   string `json:"channel_id"`
}
