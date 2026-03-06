package livekitwebhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/rtc/application"
)

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
		ReceiveEvent: func(r *http.Request, provider auth.KeyProvider) (*livekit.WebhookEvent, error) {
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
