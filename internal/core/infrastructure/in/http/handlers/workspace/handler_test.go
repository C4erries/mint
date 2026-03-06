package workspace

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/core/infrastructure/in/http/middleware"
	"github.com/c4erries/mint/internal/workspace/domain"
)

type capturedWorkspaceMessage struct {
	topic string
	key   string
	value []byte
}

type fakeWorkspacePublisher struct {
	mu       sync.Mutex
	messages []capturedWorkspaceMessage
}

func (p *fakeWorkspacePublisher) Publish(_ context.Context, topic string, key string, value []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	copied := append([]byte(nil), value...)
	p.messages = append(p.messages, capturedWorkspaceMessage{
		topic: topic,
		key:   key,
		value: copied,
	})

	return nil
}

func (p *fakeWorkspacePublisher) lastMessage(t *testing.T) capturedWorkspaceMessage {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()

	require.NotEmpty(t, p.messages)

	return p.messages[len(p.messages)-1]
}

func TestHandler_JoinWorkspaceRoute(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	publisher := &fakeWorkspacePublisher{}
	handler := NewHandler(nil, publisher, "mint.workspace.commands.v1", func() string { return "id-1" }, func() time.Time { return time.Unix(100, 0).UTC() })

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(middleware.ContextAccountIDKey, "actor-1")
		c.Next()
	})

	group := router.Group("/api/v1")
	handler.RegisterRoutes(group)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/ws-1/members/user-7/join", nil)
	request.Header.Set("X-Correlation-ID", "corr-1")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)
	require.Equal(t, http.StatusAccepted, response.Code)

	message := publisher.lastMessage(t)
	require.Equal(t, "mint.workspace.commands.v1", message.topic)
	require.Equal(t, "ws-1:user-7", message.key)

	var envelope map[string]any
	err := json.Unmarshal(message.value, &envelope)
	require.NoError(t, err)
	require.Equal(t, domain.CommandJoinWorkspace, envelope["type"])

	payload, ok := envelope["payload"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "user-7", payload["user_id"])
}

func TestHandler_OldMemberActionRouteIsRemoved(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	handler := NewHandler(nil, &fakeWorkspacePublisher{}, "mint.workspace.commands.v1", func() string { return "id-1" }, time.Now)

	router := gin.New()
	group := router.Group("/api/v1")
	handler.RegisterRoutes(group)

	request := httptest.NewRequest(http.MethodPost, "/api/v1/workspaces/ws-1/members/user-7:join", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	require.Equal(t, http.StatusNotFound, response.Code)
}
