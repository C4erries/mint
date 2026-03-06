package livekitwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/webhook"
)

// Handler validates LiveKit webhook signature and normalizes callbacks into RTC commands.
type Handler struct {
	keyProvider auth.KeyProvider
	receive     func(r *http.Request, provider auth.KeyProvider) ([]byte, error)
	commands    *application.CommandService
	logger      *slog.Logger
	now         func() time.Time
	idGenerator func() string
}

func New(apiKey string, apiSecret string, commands *application.CommandService, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	var keyProvider auth.KeyProvider
	if apiKey != "" && apiSecret != "" {
		keyProvider = auth.NewSimpleKeyProvider(apiKey, apiSecret)
	}

	return &Handler{
		keyProvider: keyProvider,
		receive:     webhook.Receive,
		commands:    commands,
		logger:      logger,
		now:         time.Now,
		idGenerator: uuid.NewString,
	}
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := h.readVerifiedBody(request)
	if err != nil {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}

	var callback callbackEvent
	if err = json.Unmarshal(body, &callback); err != nil {
		h.logger.Error("failed to decode livekit webhook event", slog.String("error", err.Error()))
		http.Error(writer, "invalid json", http.StatusBadRequest)
		return
	}

	if err = callback.validate(); err != nil {
		h.logger.Error("invalid livekit webhook payload", slog.String("error", err.Error()))
		http.Error(writer, "invalid payload", http.StatusBadRequest)
		return
	}

	if err = h.handleEvent(request.Context(), callback); err != nil {
		h.logger.Error("failed to handle livekit webhook event", slog.String("error", err.Error()))
		http.Error(writer, "unable to process callback", http.StatusInternalServerError)
		return
	}

	writer.WriteHeader(http.StatusAccepted)
}

type callbackEvent struct {
	EventID       string     `json:"event_id"`
	EventType     string     `json:"event_type"`
	WorkspaceID   string     `json:"workspace_id"`
	ChannelID     string     `json:"channel_id"`
	RoomID        string     `json:"room_id"`
	UserID        string     `json:"user_id"`
	CorrelationID string     `json:"correlation_id"`
	OccurredAt    *time.Time `json:"occurred_at"`
}

func (e callbackEvent) validate() error {
	if e.EventType == "" || e.WorkspaceID == "" || e.ChannelID == "" || e.UserID == "" {
		return application.ErrInvalidCommand
	}

	return nil
}

func (h *Handler) handleEvent(ctx context.Context, callback callbackEvent) error {
	occurredAt := h.now().UTC()
	if callback.OccurredAt != nil {
		occurredAt = callback.OccurredAt.UTC()
	}

	commandID := callback.EventID
	if commandID == "" {
		commandID = h.idGenerator()
	}

	correlationID := callback.CorrelationID
	if correlationID == "" {
		correlationID = commandID
	}

	meta := application.CommandMeta{
		CommandID:     commandID,
		CorrelationID: correlationID,
		CausationID:   callback.EventID,
		MessageID:     commandID,
		OccurredAt:    occurredAt,
		WorkspaceID:   callback.WorkspaceID,
		ChannelID:     callback.ChannelID,
		RoomID:        callback.RoomID,
		ActorID:       callback.UserID,
		SchemaVersion: 1,
	}

	switch callback.EventType {
	case "participant_joined":
		return h.commands.JoinVoiceChannel(ctx, application.JoinVoiceChannelCommand{Meta: meta, UserID: callback.UserID})
	case "participant_left":
		return h.commands.LeaveVoiceChannel(ctx, application.LeaveVoiceChannelCommand{Meta: meta, UserID: callback.UserID})
	case "participant_muted":
		return h.commands.MuteSelf(ctx, application.MuteSelfCommand{Meta: meta, UserID: callback.UserID})
	case "participant_unmuted":
		return h.commands.UnmuteSelf(ctx, application.UnmuteSelfCommand{Meta: meta, UserID: callback.UserID})
	case "camera_enabled":
		return h.commands.EnableCamera(ctx, application.EnableCameraCommand{Meta: meta, UserID: callback.UserID})
	case "camera_disabled":
		return h.commands.DisableCamera(ctx, application.DisableCameraCommand{Meta: meta, UserID: callback.UserID})
	case "room_terminated":
		return h.commands.TerminateVoiceSession(ctx, application.TerminateVoiceSessionCommand{Meta: meta})
	default:
		h.logger.Debug("skip unsupported livekit callback", slog.String("event_type", callback.EventType))
		return nil
	}
}

func (h *Handler) readVerifiedBody(request *http.Request) ([]byte, error) {
	if h.keyProvider == nil {
		h.logger.Error("livekit webhook key provider is not configured")
		return nil, errors.New("livekit key provider is required")
	}

	body, err := h.receive(request, h.keyProvider)
	if err != nil {
		h.logger.Error("failed to verify livekit webhook signature", slog.String("error", err.Error()))
		return nil, err
	}

	return body, nil
}

func IsDomainNotFound(err error) bool {
	return errors.Is(err, domain.ErrVoiceRoomNotFound) ||
		errors.Is(err, domain.ErrVoiceChannelBindingNotFound) ||
		errors.Is(err, domain.ErrParticipantNotFound)
}

func RunHealthProbe(ctx context.Context, endpoint string, timeout time.Duration) error {
	client := http.Client{Timeout: timeout}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()

	if response.StatusCode >= http.StatusBadRequest {
		return errors.New("health probe request failed")
	}

	return nil
}
