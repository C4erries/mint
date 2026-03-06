package permissionserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	permissionv1 "github.com/c4erries/mint/api/permission/v1"
	workspaceapp "github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
)

// Server is gRPC transport adapter for workspace permission checks.
type Server struct {
	permissionv1.UnimplementedPermissionServiceServer

	permissions *workspaceapp.PermissionService
}

func New(permissions *workspaceapp.PermissionService) *Server {
	return &Server{permissions: permissions}
}

func (s *Server) CanJoinVoiceChannel(ctx context.Context, request *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
	if request.GetWorkspaceId() == "" || request.GetChannelId() == "" || request.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "workspace_id, channel_id and user_id are required")
	}

	allowed, err := s.permissions.CanJoinVoiceChannel(ctx, request.GetWorkspaceId(), request.GetChannelId(), request.GetUserId())
	if err != nil {
		return nil, mapPermissionError(err)
	}

	return &permissionv1.CanJoinVoiceChannelResponse{Allowed: allowed}, nil
}

func mapPermissionError(err error) error {
	switch {
	case errors.Is(err, workspaceapp.ErrInvalidQuery):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrWorkspaceNotFound), errors.Is(err, domain.ErrChannelNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
