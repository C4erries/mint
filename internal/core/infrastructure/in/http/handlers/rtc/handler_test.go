package rtc

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
	"github.com/c4erries/mint/internal/core/infrastructure/in/http/middleware"
)

type capturedMessage struct {
	topic string
	key   string
	value []byte
}

type fakePublisher struct {
	mu       sync.Mutex
	messages []capturedMessage
}

func (p *fakePublisher) Publish(_ context.Context, topic string, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	copyValue := make([]byte, len(value))
	copy(copyValue, value)
	p.messages = append(p.messages, capturedMessage{topic: topic, key: key, value: copyValue})
	return nil
}

func (p *fakePublisher) LastMessage(t *testing.T) capturedMessage {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()

	require.NotEmpty(t, p.messages)
	return p.messages[len(p.messages)-1]
}

type noopQueryClient struct{}

func (noopQueryClient) GetVoiceRoomState(_ context.Context, _ string, _ string) (*rtcv1.GetVoiceRoomStateResponse, error) {
	return &rtcv1.GetVoiceRoomStateResponse{}, nil
}

func (noopQueryClient) ListVoiceParticipants(_ context.Context, _ string) (*rtcv1.ListVoiceParticipantsResponse, error) {
	return &rtcv1.ListVoiceParticipantsResponse{}, nil
}

func (noopQueryClient) GetVoiceChannelBinding(_ context.Context, _ string, _ string) (*rtcv1.GetVoiceChannelBindingResponse, error) {
	return &rtcv1.GetVoiceChannelBindingResponse{}, nil
}

func (noopQueryClient) GetRtcTokenGrantStatus(_ context.Context, _ string) (*rtcv1.GetRtcTokenGrantStatusResponse, error) {
	return &rtcv1.GetRtcTokenGrantStatusResponse{}, nil
}

func TestHandler_CommandMapping(t *testing.T) {
	gin.SetMode(gin.TestMode)

	now := time.Date(2026, 3, 6, 18, 0, 0, 0, time.UTC)

	testCases := []struct {
		name         string
		path         string
		body         string
		expectedType string
		expectedKey  string
	}{
		{name: "join", path: "/api/v1/workspaces/ws-1/channels/ch-1/voice/join", expectedType: commandJoinVoiceChannel, expectedKey: "ws-1:ch-1"},
		{name: "leave", path: "/api/v1/workspaces/ws-1/channels/ch-1/voice/leave", expectedType: commandLeaveVoiceChannel, expectedKey: "ws-1:ch-1"},
		{name: "mute", path: "/api/v1/workspaces/ws-1/channels/ch-1/voice/mute", expectedType: commandMuteSelf, expectedKey: "ws-1:ch-1"},
		{name: "unmute", path: "/api/v1/workspaces/ws-1/channels/ch-1/voice/unmute", expectedType: commandUnmuteSelf, expectedKey: "ws-1:ch-1"},
		{name: "camera-enable", path: "/api/v1/workspaces/ws-1/channels/ch-1/voice/camera/enable", expectedType: commandEnableCamera, expectedKey: "ws-1:ch-1"},
		{name: "camera-disable", path: "/api/v1/workspaces/ws-1/channels/ch-1/voice/camera/disable", expectedType: commandDisableCamera, expectedKey: "ws-1:ch-1"},
		{
			name:         "issue-token",
			path:         "/api/v1/workspaces/ws-1/channels/ch-1/voice/token",
			body:         `{"ttl_seconds":120,"can_publish":true,"can_subscribe":true}`,
			expectedType: commandIssueRtcToken,
			expectedKey:  "ws-1:ch-1",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			publisher := &fakePublisher{}
			handler := NewHandler(
				publisher,
				noopQueryClient{},
				"mint.rtc.commands.v1",
				func() string { return "id-fixed" },
				func() time.Time { return now },
			)

			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Set(middleware.ContextAccountIDKey, "user-1")
				c.Next()
			})

			group := router.Group("/api/v1")
			handler.RegisterRoutes(group)

			body := strings.NewReader(testCase.body)
			request := httptest.NewRequest(http.MethodPost, testCase.path, body)
			if testCase.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}

			request.Header.Set("X-Correlation-ID", "corr-1")

			responseRecorder := httptest.NewRecorder()
			router.ServeHTTP(responseRecorder, request)
			require.Equal(t, http.StatusAccepted, responseRecorder.Code)

			message := publisher.LastMessage(t)
			require.Equal(t, "mint.rtc.commands.v1", message.topic)
			require.Equal(t, testCase.expectedKey, message.key)

			var envelope map[string]any
			err := json.Unmarshal(message.value, &envelope)
			require.NoError(t, err)
			require.Equal(t, testCase.expectedType, envelope["type"])

			meta, ok := envelope["meta"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, "corr-1", meta["correlation_id"])
			require.Equal(t, "ws-1", meta["workspace_id"])
			require.Equal(t, "ch-1", meta["channel_id"])
			require.Equal(t, "user-1", meta["actor_id"])
			require.Equal(t, "", meta["room_id"])

			payload, ok := envelope["payload"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, "user-1", payload["user_id"])
		})
	}
}
