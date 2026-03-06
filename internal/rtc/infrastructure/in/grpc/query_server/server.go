package queryserver

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

// Server is gRPC query transport adapter.
type Server struct {
	rtcv1.UnimplementedRTCQueryServiceServer

	queries *application.QueryService
}

func New(queries *application.QueryService) *Server {
	return &Server{queries: queries}
}

func (s *Server) GetVoiceRoomState(ctx context.Context, request *rtcv1.GetVoiceRoomStateRequest) (*rtcv1.GetVoiceRoomStateResponse, error) {
	if err := s.ensureQueries(); err != nil {
		return nil, err
	}

	state, err := s.queries.GetVoiceRoomState(ctx, application.GetVoiceRoomStateQuery{
		WorkspaceID: request.GetWorkspaceId(),
		ChannelID:   request.GetChannelId(),
	})
	if err != nil {
		return nil, mapQueryError(err)
	}

	return &rtcv1.GetVoiceRoomStateResponse{State: mapVoiceRoomState(state)}, nil
}

func (s *Server) ListVoiceParticipants(ctx context.Context, request *rtcv1.ListVoiceParticipantsRequest) (*rtcv1.ListVoiceParticipantsResponse, error) {
	if err := s.ensureQueries(); err != nil {
		return nil, err
	}

	participants, err := s.queries.ListVoiceParticipants(ctx, application.ListVoiceParticipantsQuery{RoomID: request.GetRoomId()})
	if err != nil {
		return nil, mapQueryError(err)
	}

	response := &rtcv1.ListVoiceParticipantsResponse{Participants: make([]*rtcv1.VoiceParticipant, 0, len(participants))}
	for _, participant := range participants {
		response.Participants = append(response.Participants, mapVoiceParticipant(participant))
	}

	return response, nil
}

func (s *Server) GetVoiceChannelBinding(ctx context.Context, request *rtcv1.GetVoiceChannelBindingRequest) (*rtcv1.GetVoiceChannelBindingResponse, error) {
	if err := s.ensureQueries(); err != nil {
		return nil, err
	}

	binding, err := s.queries.GetVoiceChannelBinding(ctx, application.GetVoiceChannelBindingQuery{
		WorkspaceID: request.GetWorkspaceId(),
		ChannelID:   request.GetChannelId(),
	})
	if err != nil {
		return nil, mapQueryError(err)
	}

	return &rtcv1.GetVoiceChannelBindingResponse{Binding: mapVoiceChannelBinding(binding)}, nil
}

func (s *Server) GetRtcTokenGrantStatus(ctx context.Context, request *rtcv1.GetRtcTokenGrantStatusRequest) (*rtcv1.GetRtcTokenGrantStatusResponse, error) {
	if err := s.ensureQueries(); err != nil {
		return nil, err
	}

	grantStatus, err := s.queries.GetRtcTokenGrantStatus(ctx, application.GetRtcTokenGrantStatusQuery{TokenID: request.GetTokenId()})
	if err != nil {
		return nil, mapQueryError(err)
	}

	return &rtcv1.GetRtcTokenGrantStatusResponse{Status: mapRtcTokenGrantStatus(grantStatus)}, nil
}

func mapVoiceRoomState(state application.VoiceRoomState) *rtcv1.VoiceRoomState {
	return &rtcv1.VoiceRoomState{
		RoomId:            state.RoomID,
		WorkspaceId:       state.WorkspaceID,
		ChannelId:         state.ChannelID,
		Active:            state.Active,
		ParticipantCount:  int32(state.ParticipantCount),
		LastStateChangeAt: timestamppb.New(state.LastStateChangeAt),
	}
}

func mapVoiceParticipant(participant application.VoiceParticipant) *rtcv1.VoiceParticipant {
	mapped := &rtcv1.VoiceParticipant{
		UserId:          participant.UserID,
		JoinedAt:        timestamppb.New(participant.JoinedAt),
		MicrophoneMuted: participant.MicrophoneMuted,
		CameraEnabled:   participant.CameraEnabled,
	}

	if participant.LeftAt != nil {
		mapped.LeftAt = timestamppb.New(*participant.LeftAt)
	}

	return mapped
}

func mapVoiceChannelBinding(binding application.VoiceChannelBindingView) *rtcv1.VoiceChannelBinding {
	return &rtcv1.VoiceChannelBinding{
		WorkspaceId: binding.WorkspaceID,
		ChannelId:   binding.ChannelID,
		RoomId:      binding.RoomID,
		UpdatedAt:   timestamppb.New(binding.UpdatedAt),
	}
}

func mapRtcTokenGrantStatus(statusView application.RtcTokenGrantStatus) *rtcv1.RtcTokenGrantStatus {
	return &rtcv1.RtcTokenGrantStatus{
		TokenId:      statusView.TokenID,
		RoomId:       statusView.RoomID,
		UserId:       statusView.UserID,
		IssuedAt:     timestamppb.New(statusView.IssuedAt),
		ExpiresAt:    timestamppb.New(statusView.ExpiresAt),
		Expired:      statusView.Expired,
		CanPublish:   statusView.CanPublish,
		CanSubscribe: statusView.CanSubscribe,
		Token:        statusView.Token,
	}
}

func (s *Server) ensureQueries() error {
	if s == nil || s.queries == nil {
		return status.Error(codes.Unavailable, "rtc query service is not configured")
	}

	return nil
}

func mapQueryError(err error) error {
	switch {
	case errors.Is(err, application.ErrInvalidQuery):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrVoiceRoomNotFound),
		errors.Is(err, domain.ErrVoiceChannelBindingNotFound),
		errors.Is(err, domain.ErrGrantNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrGrantExpired):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
