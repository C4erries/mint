package workspace

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/c4erries/mint/internal/core/infrastructure/in/http/middleware"
	workspaceapp "github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
)

// CommandPublisher publishes workspace commands to Kafka.
type CommandPublisher interface {
	Publish(ctx context.Context, topic string, key string, value []byte) error
}

// Handler serves workspace REST endpoints.
type Handler struct {
	queries       *workspaceapp.QueryService
	publisher     CommandPublisher
	commandsTopic string
	idGenerator   func() string
	now           func() time.Time
}

func NewHandler(
	queries *workspaceapp.QueryService,
	publisher CommandPublisher,
	commandsTopic string,
	idGenerator func() string,
	now func() time.Time,
) *Handler {
	return &Handler{
		queries:       queries,
		publisher:     publisher,
		commandsTopic: commandsTopic,
		idGenerator:   idGenerator,
		now:           now,
	}
}

func (h *Handler) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/workspaces", h.createWorkspace)
	group.POST("/workspaces/:workspace_id/channels", h.createChannel)
	group.POST("/workspaces/:workspace_id/members/:user_action", h.joinWorkspace)
	group.GET("/workspaces/:workspace_id", h.getWorkspace)
	group.GET("/workspaces/:workspace_id/channels/:channel_id", h.getChannel)
}

type createWorkspaceRequest struct {
	Name string `json:"name"`
}

type createChannelRequest struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

func (h *Handler) createWorkspace(c *gin.Context) {
	var request createWorkspaceRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	workspaceID := h.idGenerator()
	commandMeta := h.buildMeta(c, workspaceID, "")

	payload := map[string]any{"workspace_name": strings.TrimSpace(request.Name)}
	if payload["workspace_name"] == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workspace name is required"})
		return
	}

	if err := h.publishCommand(c.Request.Context(), domain.CommandCreateWorkspace, commandMeta, payload, workspaceID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish command"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"command_id":     commandMeta.CommandID,
		"correlation_id": commandMeta.CorrelationID,
		"workspace_id":   workspaceID,
	})
}

func (h *Handler) createChannel(c *gin.Context) {
	var request createChannelRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	workspaceID := c.Param("workspace_id")
	if workspaceID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workspace_id is required"})
		return
	}

	channelID := h.idGenerator()
	commandMeta := h.buildMeta(c, workspaceID, channelID)

	payload := map[string]any{"channel_name": strings.TrimSpace(request.Name), "channel_kind": strings.TrimSpace(request.Kind)}
	if payload["channel_name"] == "" || payload["channel_kind"] == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "channel name and kind are required"})
		return
	}

	if err := h.publishCommand(c.Request.Context(), domain.CommandCreateChannel, commandMeta, payload, workspaceID+":"+channelID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish command"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"command_id":     commandMeta.CommandID,
		"correlation_id": commandMeta.CorrelationID,
		"workspace_id":   workspaceID,
		"channel_id":     channelID,
	})
}

func (h *Handler) joinWorkspace(c *gin.Context) {
	workspaceID := c.Param("workspace_id")

	action := c.Param("user_action")
	if workspaceID == "" || action == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workspace_id and user action are required"})
		return
	}

	parts := strings.Split(action, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "join" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported member action"})
		return
	}

	userID := parts[0]
	commandMeta := h.buildMeta(c, workspaceID, "")

	payload := map[string]any{"user_id": userID}
	if err := h.publishCommand(c.Request.Context(), domain.CommandJoinWorkspace, commandMeta, payload, workspaceID+":"+userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish command"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"command_id":     commandMeta.CommandID,
		"correlation_id": commandMeta.CorrelationID,
		"workspace_id":   workspaceID,
		"user_id":        userID,
	})
}

func (h *Handler) getWorkspace(c *gin.Context) {
	workspace, err := h.queries.GetWorkspace(c.Request.Context(), workspaceapp.GetWorkspaceQuery{WorkspaceID: c.Param("workspace_id")})
	if err != nil {
		h.writeQueryError(c, err)
		return
	}

	c.JSON(http.StatusOK, workspace)
}

func (h *Handler) getChannel(c *gin.Context) {
	channel, err := h.queries.GetChannel(c.Request.Context(), workspaceapp.GetChannelQuery{
		WorkspaceID: c.Param("workspace_id"),
		ChannelID:   c.Param("channel_id"),
	})
	if err != nil {
		h.writeQueryError(c, err)
		return
	}

	c.JSON(http.StatusOK, channel)
}

func (h *Handler) publishCommand(ctx context.Context, commandType string, meta workspaceapp.CommandMeta, payload any, key string) error {
	envelope := map[string]any{
		"type":    commandType,
		"meta":    meta,
		"payload": payload,
	}

	rawMessage, err := json.Marshal(envelope)
	if err != nil {
		return err
	}

	return h.publisher.Publish(ctx, h.commandsTopic, key, rawMessage)
}

func (h *Handler) buildMeta(c *gin.Context, workspaceID string, channelID string) workspaceapp.CommandMeta {
	correlationID := strings.TrimSpace(c.GetHeader("X-Correlation-ID"))
	if correlationID == "" {
		correlationID = h.idGenerator()
	}

	commandID := h.idGenerator()
	messageID := h.idGenerator()

	return workspaceapp.CommandMeta{
		CommandID:     commandID,
		CorrelationID: correlationID,
		CausationID:   messageID,
		MessageID:     messageID,
		OccurredAt:    h.now().UTC(),
		WorkspaceID:   workspaceID,
		ChannelID:     channelID,
		ActorID:       middleware.MustAccountID(c),
		SchemaVersion: 1,
	}
}

func (h *Handler) writeQueryError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, workspaceapp.ErrInvalidQuery):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, domain.ErrWorkspaceNotFound), errors.Is(err, domain.ErrChannelNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal server error"})
	}
}
