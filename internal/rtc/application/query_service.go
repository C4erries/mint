package application

import (
	"context"
	"fmt"
	"time"
)

// QueryService serves read-side queries directly from read model.
type QueryService struct {
	rooms  VoiceRoomReadRepository
	grants MediaAccessGrantRepository
	now    func() time.Time
}

func NewQueryService(rooms VoiceRoomReadRepository, grants MediaAccessGrantRepository) *QueryService {
	return &QueryService{
		rooms:  rooms,
		grants: grants,
		now:    time.Now,
	}
}

func (s *QueryService) GetVoiceRoomState(ctx context.Context, query GetVoiceRoomStateQuery) (VoiceRoomState, error) {
	if err := query.Validate(); err != nil {
		return VoiceRoomState{}, err
	}

	state, err := s.rooms.GetVoiceRoomState(ctx, query.WorkspaceID, query.ChannelID)
	if err != nil {
		return VoiceRoomState{}, fmt.Errorf("get voice room state: %w", err)
	}

	return state, nil
}

func (s *QueryService) ListVoiceParticipants(ctx context.Context, query ListVoiceParticipantsQuery) ([]VoiceParticipant, error) {
	if err := query.Validate(); err != nil {
		return nil, err
	}

	participants, err := s.rooms.ListVoiceParticipants(ctx, query.RoomID)
	if err != nil {
		return nil, fmt.Errorf("list voice participants: %w", err)
	}

	return participants, nil
}

func (s *QueryService) GetVoiceChannelBinding(ctx context.Context, query GetVoiceChannelBindingQuery) (VoiceChannelBindingView, error) {
	if err := query.Validate(); err != nil {
		return VoiceChannelBindingView{}, err
	}

	binding, err := s.rooms.GetVoiceChannelBinding(ctx, query.WorkspaceID, query.ChannelID)
	if err != nil {
		return VoiceChannelBindingView{}, fmt.Errorf("get voice channel binding: %w", err)
	}

	return binding, nil
}

func (s *QueryService) GetRtcTokenGrantStatus(ctx context.Context, query GetRtcTokenGrantStatusQuery) (RtcTokenGrantStatus, error) {
	if err := query.Validate(); err != nil {
		return RtcTokenGrantStatus{}, err
	}

	grant, err := s.grants.GetGrant(ctx, query.TokenID)
	if err != nil {
		return RtcTokenGrantStatus{}, fmt.Errorf("get rtc token grant status: %w", err)
	}

	return RtcTokenGrantStatus{
		TokenID:      grant.TokenID,
		RoomID:       grant.RoomID,
		UserID:       grant.UserID,
		IssuedAt:     grant.IssuedAt,
		ExpiresAt:    grant.ExpiresAt,
		Expired:      grant.IsExpired(s.now()),
		CanPublish:   grant.CanPublish,
		CanSubscribe: grant.CanSubscribe,
	}, nil
}
