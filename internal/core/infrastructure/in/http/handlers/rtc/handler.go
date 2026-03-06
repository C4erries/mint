package rtc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
	"github.com/c4erries/mint/internal/core/infrastructure/in/http/middleware"
	workspaceapp "github.com/c4erries/mint/internal/workspace/application"
)

const (
	commandJoinVoiceChannel  = "JoinVoiceChannel"
	commandLeaveVoiceChannel = "LeaveVoiceChannel"
	commandMuteSelf          = "MuteSelf"
	commandUnmuteSelf        = "UnmuteSelf"
	commandEnableCamera      = "EnableCamera"
	commandDisableCamera     = "DisableCamera"
	commandIssueRtcToken     = "IssueRtcToken"
)

// CommandPublisher publishes RTC commands to Kafka.
type CommandPublisher interface {
	Publish(ctx context.Context, topic string, key string, value []byte) error
}

// QueryClient reads RTC state via gRPC.
type QueryClient interface {
	GetVoiceRoomState(ctx context.Context, workspaceID string, channelID string) (*rtcv1.GetVoiceRoomStateResponse, error)
	ListVoiceParticipants(ctx context.Context, roomID string) (*rtcv1.ListVoiceParticipantsResponse, error)
	GetVoiceChannelBinding(ctx context.Context, workspaceID string, channelID string) (*rtcv1.GetVoiceChannelBindingResponse, error)
	GetRtcTokenGrantStatus(ctx context.Context, tokenID string) (*rtcv1.GetRtcTokenGrantStatusResponse, error)
}

// Handler serves RTC BFF endpoints.
type Handler struct {
	publisher     CommandPublisher
	queryClient   QueryClient
	commandsTopic string
	idGenerator   func() string
	now           func() time.Time
}

func NewHandler(
	publisher CommandPublisher,
	queryClient QueryClient,
	commandsTopic string,
	idGenerator func() string,
	now func() time.Time,
) *Handler {
	return &Handler{
		publisher:     publisher,
		queryClient:   queryClient,
		commandsTopic: commandsTopic,
		idGenerator:   idGenerator,
		now:           now,
	}
}

func (h *Handler) RegisterRoutes(group *gin.RouterGroup) {
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/join", h.joinVoice)
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/leave", h.leaveVoice)
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/mute", h.muteSelf)
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/unmute", h.unmuteSelf)
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/camera/enable", h.enableCamera)
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/camera/disable", h.disableCamera)
	group.POST("/workspaces/:workspace_id/channels/:channel_id/voice/token", h.issueToken)

	group.GET("/workspaces/:workspace_id/channels/:channel_id/voice/state", h.getVoiceState)
	group.GET("/workspaces/:workspace_id/channels/:channel_id/voice/binding", h.getVoiceBinding)
	group.GET("/rtc/rooms/:room_id/participants", h.listVoiceParticipants)
	group.GET("/rtc/token-grants/:token_id", h.getTokenGrantStatus)
}

func (h *Handler) joinVoice(c *gin.Context) {
	h.publishSimpleUserCommand(c, commandJoinVoiceChannel)
}

func (h *Handler) leaveVoice(c *gin.Context) {
	h.publishSimpleUserCommand(c, commandLeaveVoiceChannel)
}

func (h *Handler) muteSelf(c *gin.Context) {
	h.publishSimpleUserCommand(c, commandMuteSelf)
}

func (h *Handler) unmuteSelf(c *gin.Context) {
	h.publishSimpleUserCommand(c, commandUnmuteSelf)
}

func (h *Handler) enableCamera(c *gin.Context) {
	h.publishSimpleUserCommand(c, commandEnableCamera)
}

func (h *Handler) disableCamera(c *gin.Context) {
	h.publishSimpleUserCommand(c, commandDisableCamera)
}

type issueTokenRequest struct {
	TTLSeconds   int64 `json:"ttl_seconds"`
	CanPublish   bool  `json:"can_publish"`
	CanSubscribe bool  `json:"can_subscribe"`
}

func (h *Handler) issueToken(c *gin.Context) {
	workspaceID := c.Param("workspace_id")

	channelID := c.Param("channel_id")
	if workspaceID == "" || channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workspace_id and channel_id are required"})
		return
	}

	var request issueTokenRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	if request.TTLSeconds < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "ttl_seconds must be >= 0"})
		return
	}

	meta := h.buildMeta(c, workspaceID, channelID)
	payload := map[string]any{
		"user_id":       middleware.MustAccountID(c),
		"ttl_seconds":   request.TTLSeconds,
		"can_publish":   request.CanPublish,
		"can_subscribe": request.CanSubscribe,
	}

	if err := h.publishCommand(c.Request.Context(), commandIssueRtcToken, meta, payload, workspaceID+":"+channelID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish rtc command"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"command_id": meta.CommandID, "correlation_id": meta.CorrelationID})
}

func (h *Handler) publishSimpleUserCommand(c *gin.Context, commandType string) {
	workspaceID := c.Param("workspace_id")

	channelID := c.Param("channel_id")
	if workspaceID == "" || channelID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workspace_id and channel_id are required"})
		return
	}

	meta := h.buildMeta(c, workspaceID, channelID)
	payload := map[string]any{"user_id": middleware.MustAccountID(c)}

	if err := h.publishCommand(c.Request.Context(), commandType, meta, payload, workspaceID+":"+channelID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to publish rtc command"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"command_id": meta.CommandID, "correlation_id": meta.CorrelationID})
}

func (h *Handler) publishCommand(ctx context.Context, commandType string, meta workspaceapp.CommandMeta, payload map[string]any, key string) error {
	if h.publisher == nil {
		return errors.New("rtc command publisher is not configured")
	}

	envelope := map[string]any{
		"type": commandType,
		"meta": map[string]any{
			"command_id":     meta.CommandID,
			"correlation_id": meta.CorrelationID,
			"causation_id":   meta.CausationID,
			"message_id":     meta.MessageID,
			"occurred_at":    meta.OccurredAt,
			"workspace_id":   meta.WorkspaceID,
			"channel_id":     meta.ChannelID,
			"room_id":        "",
			"actor_id":       meta.ActorID,
			"schema_version": meta.SchemaVersion,
		},
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

	messageID := h.idGenerator()

	return workspaceapp.CommandMeta{
		CommandID:     h.idGenerator(),
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

func (h *Handler) getVoiceState(c *gin.Context) {
	if h.queryClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rtc query client is not configured"})
		return
	}

	response, err := h.queryClient.GetVoiceRoomState(c.Request.Context(), c.Param("workspace_id"), c.Param("channel_id"))
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	c.JSON(http.StatusOK, response)
}

func (h *Handler) getVoiceBinding(c *gin.Context) {
	if h.queryClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rtc query client is not configured"})
		return
	}

	response, err := h.queryClient.GetVoiceChannelBinding(c.Request.Context(), c.Param("workspace_id"), c.Param("channel_id"))
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	c.JSON(http.StatusOK, response)
}

func (h *Handler) listVoiceParticipants(c *gin.Context) {
	if h.queryClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rtc query client is not configured"})
		return
	}

	response, err := h.queryClient.ListVoiceParticipants(c.Request.Context(), c.Param("room_id"))
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	c.JSON(http.StatusOK, response)
}

func (h *Handler) getTokenGrantStatus(c *gin.Context) {
	if h.queryClient == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "rtc query client is not configured"})
		return
	}

	response, err := h.queryClient.GetRtcTokenGrantStatus(c.Request.Context(), c.Param("token_id"))
	if err != nil {
		writeGRPCError(c, err)
		return
	}

	c.JSON(http.StatusOK, response)
}

func writeGRPCError(c *gin.Context, err error) {
	grpcStatus, ok := status.FromError(err)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	switch grpcStatus.Code() {
	case codes.InvalidArgument:
		c.JSON(http.StatusBadRequest, gin.H{"error": grpcStatus.Message()})
	case codes.NotFound:
		c.JSON(http.StatusNotFound, gin.H{"error": grpcStatus.Message()})
	case codes.PermissionDenied:
		c.JSON(http.StatusForbidden, gin.H{"error": grpcStatus.Message()})
	case codes.FailedPrecondition:
		c.JSON(http.StatusConflict, gin.H{"error": grpcStatus.Message()})
	case codes.DeadlineExceeded:
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": grpcStatus.Message()})
	default:
		c.JSON(http.StatusBadGateway, gin.H{"error": grpcStatus.Message()})
	}
}
