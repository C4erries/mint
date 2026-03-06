package livekitwebhook

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

const (
	defaultMaxBodyBytes   int64 = 1 << 20
	defaultProcessTimeout       = 2 * time.Second
	defaultReplayWindow         = 15 * time.Minute
	fallbackActorID             = "livekit-webhook"
)

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
