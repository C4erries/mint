package application

import (
	"context"
	"errors"

	"github.com/c4erries/mint/internal/workspace/domain"
)

// QueryService handles workspace read use cases.
type QueryService struct {
	readRepo ReadRepository
}

func NewQueryService(readRepo ReadRepository) *QueryService {
	return &QueryService{readRepo: readRepo}
}

func (s *QueryService) GetWorkspace(ctx context.Context, query GetWorkspaceQuery) (WorkspaceView, error) {
	if err := query.Validate(); err != nil {
		return WorkspaceView{}, err
	}

	workspace, err := s.readRepo.GetWorkspace(ctx, query.WorkspaceID)
	if err != nil {
		if errors.Is(err, domain.ErrWorkspaceNotFound) {
			return WorkspaceView{}, domain.ErrWorkspaceNotFound
		}

		return WorkspaceView{}, err
	}

	return workspace, nil
}

func (s *QueryService) GetChannel(ctx context.Context, query GetChannelQuery) (ChannelView, error) {
	if err := query.Validate(); err != nil {
		return ChannelView{}, err
	}

	channel, err := s.readRepo.GetChannel(ctx, query.WorkspaceID, query.ChannelID)
	if err != nil {
		if errors.Is(err, domain.ErrChannelNotFound) {
			return ChannelView{}, domain.ErrChannelNotFound
		}

		return ChannelView{}, err
	}

	return channel, nil
}
