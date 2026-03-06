package livekitwebhook

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/webhook"
	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	inmemoryrepo "github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/inmemory"
)

type flakyWebhookWriteRepository struct {
	inner         application.VoiceRoomWriteRepository
	failRemaining int
}

func (r *flakyWebhookWriteRepository) WithTx(ctx context.Context, fn func(tx application.VoiceRoomWriteTx) error) error {
	if r.failRemaining > 0 {
		r.failRemaining--
		return errors.New("transient write error")
	}

	return r.inner.WithTx(ctx, fn)
}

type allowWebhookPermissionChecker struct{}

func (allowWebhookPermissionChecker) CanJoinVoiceChannel(_ context.Context, _, _, _ string) (bool, error) {
	return true, nil
}

type noopWebhookLiveKitClient struct{}

func (noopWebhookLiveKitClient) IssueToken(_ context.Context, _ application.LiveKitTokenRequest) (application.LiveKitIssuedToken, error) {
	now := time.Now().UTC()
	return application.LiveKitIssuedToken{TokenID: "token-id", Token: "token", IssuedAt: now, ExpiresAt: now.Add(time.Minute)}, nil
}

func TestRoomScope_FromRoomMetadata(t *testing.T) {
	t.Parallel()

	event := &livekit.WebhookEvent{
		Room: &livekit.Room{Metadata: `{"workspace_id":"ws-1","channel_id":"ch-1"}`},
	}

	workspaceID, channelID, err := roomScope(event)
	require.NoError(t, err)
	require.Equal(t, "ws-1", workspaceID)
	require.Equal(t, "ch-1", channelID)
}

func TestInMemoryReplayProtector_MarkIfNew(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	protector := NewInMemoryReplayProtector(func() time.Time { return now })

	fresh, err := protector.MarkIfNew(context.Background(), "evt-1", time.Minute)
	require.NoError(t, err)
	require.True(t, fresh)

	fresh, err = protector.MarkIfNew(context.Background(), "evt-1", time.Minute)
	require.NoError(t, err)
	require.False(t, fresh)
}

func TestHandler_ServeHTTP_DuplicateEventAccepted(t *testing.T) {
	t.Parallel()

	event := &livekit.WebhookEvent{
		Id:    "evt-1",
		Event: "room_started",
		Room: &livekit.Room{
			Name:     "room-1",
			Metadata: `{"workspace_id":"ws-1","channel_id":"ch-1"}`,
		},
	}

	handler := NewWithOptions("api-key", "api-secret", nil, nil, Options{
		Now: func() time.Time { return time.Unix(0, 0).UTC() },
		ReceiveEvent: func(_ *http.Request, _ auth.KeyProvider) (*livekit.WebhookEvent, error) {
			return event, nil
		},
	})

	request := httptest.NewRequest(http.MethodPost, "/livekit/webhook", http.NoBody)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusAccepted, response.Code)

	request2 := httptest.NewRequest(http.MethodPost, "/livekit/webhook", http.NoBody)
	response2 := httptest.NewRecorder()
	handler.ServeHTTP(response2, request2)
	require.Equal(t, http.StatusAccepted, response2.Code)
}

func TestHandler_ServeHTTP_RetryAfterFailedHandleProcessesEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	baseStore := inmemoryrepo.NewRoomStore()
	err := baseStore.WithTx(ctx, func(tx application.VoiceRoomWriteTx) error {
		room, createErr := domain.NewVoiceRoom("room-1", "ws-1", "ch-1", now)
		if createErr != nil {
			return createErr
		}

		return tx.SaveRoom(ctx, room)
	})
	require.NoError(t, err)

	flakyStore := &flakyWebhookWriteRepository{inner: baseStore, failRemaining: 1}
	commands, err := application.NewCommandService(
		flakyStore,
		inmemoryrepo.NewGrantStore(func() time.Time { return now }),
		allowWebhookPermissionChecker{},
		noopWebhookLiveKitClient{},
		application.CommandServiceOptions{
			DefaultTokenTTL: time.Minute,
			Now:             func() time.Time { return now },
		},
	)
	require.NoError(t, err)

	event := &livekit.WebhookEvent{
		Id:        "evt-retry-1",
		Event:     webhook.EventRoomFinished,
		CreatedAt: now.Unix(),
		Room: &livekit.Room{
			Name:     "room-1",
			Metadata: `{"workspace_id":"ws-1","channel_id":"ch-1"}`,
		},
	}

	handler := NewWithOptions("api-key", "api-secret", commands, nil, Options{
		Now: func() time.Time { return now },
		ReceiveEvent: func(_ *http.Request, _ auth.KeyProvider) (*livekit.WebhookEvent, error) {
			return event, nil
		},
	})

	first := httptest.NewRecorder()
	handler.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/livekit/webhook", http.NoBody))
	require.Equal(t, http.StatusInternalServerError, first.Code)

	second := httptest.NewRecorder()
	handler.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/livekit/webhook", http.NoBody))
	require.Equal(t, http.StatusAccepted, second.Code)

	roomActive := true
	err = baseStore.WithTx(ctx, func(tx application.VoiceRoomWriteTx) error {
		room, getErr := tx.GetRoom(ctx, "room-1")
		if getErr != nil {
			return getErr
		}

		roomActive = room.Active

		return nil
	})
	require.NoError(t, err)
	require.False(t, roomActive)
}

func TestHandler_ServeHTTP_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	handler := NewWithOptions("api-key", "api-secret", nil, nil, Options{})
	request := httptest.NewRequest(http.MethodGet, "/livekit/webhook", http.NoBody)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusMethodNotAllowed, response.Code)
}

func TestRoomScope_FallbackToParticipantAttributes(t *testing.T) {
	t.Parallel()

	event := &livekit.WebhookEvent{
		Room: &livekit.Room{},
		Participant: &livekit.ParticipantInfo{
			Attributes: map[string]string{
				"workspace_id": "ws-attr",
				"channel_id":   "ch-attr",
			},
		},
	}

	workspaceID, channelID, err := roomScope(event)
	require.NoError(t, err)
	require.Equal(t, "ws-attr", workspaceID)
	require.Equal(t, "ch-attr", channelID)
}

func TestRoomScope_MissingScopeReturnsInvalidCommand(t *testing.T) {
	t.Parallel()

	event := &livekit.WebhookEvent{Room: &livekit.Room{}}
	_, _, err := roomScope(event)
	require.ErrorIs(t, err, application.ErrInvalidCommand)
}
