package queryserver

import (
	"context"

	"github.com/c4erries/mint/internal/rtc/application"
)

// Server is transport-neutral gRPC query adapter.
type Server struct {
	queries *application.QueryService
}

func New(queries *application.QueryService) *Server {
	return &Server{queries: queries}
}

type GetVoiceRoomStateRequest struct {
	WorkspaceID string
	ChannelID   string
}

type GetVoiceRoomStateResponse struct {
	State application.VoiceRoomState
}

func (s *Server) GetVoiceRoomState(ctx context.Context, request GetVoiceRoomStateRequest) (GetVoiceRoomStateResponse, error) {
	state, err := s.queries.GetVoiceRoomState(ctx, application.GetVoiceRoomStateQuery{
		WorkspaceID: request.WorkspaceID,
		ChannelID:   request.ChannelID,
	})
	if err != nil {
		return GetVoiceRoomStateResponse{}, err
	}

	return GetVoiceRoomStateResponse{State: state}, nil
}

type ListVoiceParticipantsRequest struct {
	RoomID string
}

type ListVoiceParticipantsResponse struct {
	Participants []application.VoiceParticipant
}

func (s *Server) ListVoiceParticipants(ctx context.Context, request ListVoiceParticipantsRequest) (ListVoiceParticipantsResponse, error) {
	participants, err := s.queries.ListVoiceParticipants(ctx, application.ListVoiceParticipantsQuery{RoomID: request.RoomID})
	if err != nil {
		return ListVoiceParticipantsResponse{}, err
	}

	return ListVoiceParticipantsResponse{Participants: participants}, nil
}

type GetVoiceChannelBindingRequest struct {
	WorkspaceID string
	ChannelID   string
}

type GetVoiceChannelBindingResponse struct {
	Binding application.VoiceChannelBindingView
}

func (s *Server) GetVoiceChannelBinding(ctx context.Context, request GetVoiceChannelBindingRequest) (GetVoiceChannelBindingResponse, error) {
	binding, err := s.queries.GetVoiceChannelBinding(ctx, application.GetVoiceChannelBindingQuery{
		WorkspaceID: request.WorkspaceID,
		ChannelID:   request.ChannelID,
	})
	if err != nil {
		return GetVoiceChannelBindingResponse{}, err
	}

	return GetVoiceChannelBindingResponse{Binding: binding}, nil
}

type GetRtcTokenGrantStatusRequest struct {
	TokenID string
}

type GetRtcTokenGrantStatusResponse struct {
	Status application.RtcTokenGrantStatus
}

func (s *Server) GetRtcTokenGrantStatus(ctx context.Context, request GetRtcTokenGrantStatusRequest) (GetRtcTokenGrantStatusResponse, error) {
	status, err := s.queries.GetRtcTokenGrantStatus(ctx, application.GetRtcTokenGrantStatusQuery{TokenID: request.TokenID})
	if err != nil {
		return GetRtcTokenGrantStatusResponse{}, err
	}

	return GetRtcTokenGrantStatusResponse{Status: status}, nil
}
