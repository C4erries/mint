package livekitwebhook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"
)

const (
	defaultMaxBodyBytes   int64 = 1 << 20
	defaultProcessTimeout       = 2 * time.Second
	defaultReplayWindow         = 15 * time.Minute
	fallbackActorID             = "livekit-webhook"
)

// ReplayProtector deduplicates webhook events by event id.
type ReplayProtector interface {
	MarkIfNew(ctx context.Context, eventID string, ttl time.Duration) (bool, error)
}

// Options configure handler hardening knobs.
type Options struct {
	MaxBodyBytes    int64
	ProcessTimeout  time.Duration
	ReplayWindow    time.Duration
	ReplayProtector ReplayProtector
	Now             func() time.Time
	IDGenerator     func() string
	ReceiveEvent    func(r *http.Request, provider auth.KeyProvider) (*livekit.WebhookEvent, error)
}

// Handler validates LiveKit webhook signature and normalizes callbacks into RTC commands.
type Handler struct {
	keyProvider auth.KeyProvider
	receive     func(r *http.Request, provider auth.KeyProvider) (*livekit.WebhookEvent, error)
	commands    *application.CommandService
	logger      *slog.Logger
	now         func() time.Time
	idGenerator func() string

	maxBodyBytes   int64
	processTimeout time.Duration
	replayWindow   time.Duration
	replay         ReplayProtector
}

func New(apiKey string, apiSecret string, commands *application.CommandService, logger *slog.Logger) *Handler {
	return NewWithOptions(apiKey, apiSecret, commands, logger, Options{})
}

func NewWithOptions(
	apiKey string,
	apiSecret string,
	commands *application.CommandService,
	logger *slog.Logger,
	options Options,
) *Handler {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	var keyProvider auth.KeyProvider
	if apiKey != "" && apiSecret != "" {
		keyProvider = auth.NewSimpleKeyProvider(apiKey, apiSecret)
	}

	if options.Now == nil {
		options.Now = time.Now
	}

	if options.IDGenerator == nil {
		options.IDGenerator = uuid.NewString
	}

	if options.ReceiveEvent == nil {
		options.ReceiveEvent = webhook.ReceiveWebhookEvent
	}

	if options.MaxBodyBytes <= 0 {
		options.MaxBodyBytes = defaultMaxBodyBytes
	}

	if options.ProcessTimeout <= 0 {
		options.ProcessTimeout = defaultProcessTimeout
	}

	if options.ReplayWindow <= 0 {
		options.ReplayWindow = defaultReplayWindow
	}

	if options.ReplayProtector == nil {
		options.ReplayProtector = NewInMemoryReplayProtector(options.Now)
	}

	return &Handler{
		keyProvider:    keyProvider,
		receive:        options.ReceiveEvent,
		commands:       commands,
		logger:         logger,
		now:            options.Now,
		idGenerator:    options.IDGenerator,
		maxBodyBytes:   options.MaxBodyBytes,
		processTimeout: options.ProcessTimeout,
		replayWindow:   options.ReplayWindow,
		replay:         options.ReplayProtector,
	}
}

func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(request.Context(), h.processTimeout)
	defer cancel()

	request = request.WithContext(ctx)
	request.Body = http.MaxBytesReader(writer, request.Body, h.maxBodyBytes)

	event, err := h.readVerifiedEvent(request)
	if err != nil {
		h.handleVerificationError(writer, err)
		return
	}

	if err = h.validateEvent(event); err != nil {
		h.logger.Warn("invalid livekit webhook payload", slog.String("error", err.Error()))
		http.Error(writer, "invalid payload", http.StatusBadRequest)
		return
	}

	if event.GetId() == "" {
		http.Error(writer, "missing event id", http.StatusBadRequest)
		return
	}

	fresh, err := h.replay.MarkIfNew(ctx, event.GetId(), h.replayWindow)
	if err != nil {
		h.logger.Error("failed to reserve livekit webhook event id", slog.String("error", err.Error()), slog.String("event_id", event.GetId()))
		http.Error(writer, "unable to reserve webhook event", http.StatusInternalServerError)
		return
	}

	if !fresh {
		writer.WriteHeader(http.StatusAccepted)
		return
	}

	if err = h.handleEvent(ctx, event); err != nil {
		h.handleCommandError(writer, err)
		return
	}

	writer.WriteHeader(http.StatusAccepted)
}

func (h *Handler) handleVerificationError(writer http.ResponseWriter, err error) {
	var bodyLimitErr *http.MaxBytesError
	if errors.As(err, &bodyLimitErr) {
		http.Error(writer, "payload too large", http.StatusRequestEntityTooLarge)
		return
	}

	if errors.Is(err, webhook.ErrNoAuthHeader) || errors.Is(err, webhook.ErrSecretNotFound) || errors.Is(err, webhook.ErrInvalidChecksum) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}

	h.logger.Error("failed to verify livekit webhook signature", slog.String("error", err.Error()))
	http.Error(writer, "unable to verify webhook", http.StatusInternalServerError)
}

func (h *Handler) handleCommandError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		http.Error(writer, "webhook timeout", http.StatusGatewayTimeout)
	case errors.Is(err, application.ErrInvalidCommand):
		http.Error(writer, "invalid payload", http.StatusBadRequest)
	case IsDomainNotFound(err), errors.Is(err, application.ErrPermissionDenied):
		// Accept stale/out-of-order callbacks to avoid retry storm.
		writer.WriteHeader(http.StatusAccepted)
	default:
		h.logger.Error("failed to handle livekit webhook event", slog.String("error", err.Error()))
		http.Error(writer, "unable to process callback", http.StatusInternalServerError)
	}
}

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

func (h *Handler) readVerifiedEvent(request *http.Request) (*livekit.WebhookEvent, error) {
	if h.keyProvider == nil {
		h.logger.Error("livekit webhook key provider is not configured")
		return nil, errors.New("livekit key provider is required")
	}

	event, err := h.receive(request, h.keyProvider)
	if err != nil {
		return nil, err
	}

	return event, nil
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

type roomMetadata struct {
	WorkspaceID string `json:"workspace_id"`
	ChannelID   string `json:"channel_id"`
}

type inMemoryReplayProtector struct {
	mu      sync.Mutex
	now     func() time.Time
	records map[string]time.Time
}

func NewInMemoryReplayProtector(now func() time.Time) ReplayProtector {
	if now == nil {
		now = time.Now
	}

	return &inMemoryReplayProtector{
		now:     now,
		records: make(map[string]time.Time),
	}
}

func (p *inMemoryReplayProtector) MarkIfNew(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if strings.TrimSpace(eventID) == "" {
		return false, application.ErrInvalidCommand
	}

	now := p.now().UTC()
	expiresAt := now.Add(ttl)

	p.mu.Lock()
	defer p.mu.Unlock()

	for key, value := range p.records {
		if !value.After(now) {
			delete(p.records, key)
		}
	}

	if existing, exists := p.records[eventID]; exists && existing.After(now) {
		return false, nil
	}

	p.records[eventID] = expiresAt
	return true, nil
}
