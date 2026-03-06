package application

import (
	"context"
	"fmt"
)

// PermissionService resolves workspace voice join decisions.
type PermissionService struct {
	readRepo  ReadRepository
	evaluator PermissionEvaluator
}

func NewPermissionService(readRepo ReadRepository, evaluator PermissionEvaluator) (*PermissionService, error) {
	if readRepo == nil || evaluator == nil {
		return nil, fmt.Errorf("permission service dependencies are required")
	}

	return &PermissionService{readRepo: readRepo, evaluator: evaluator}, nil
}

func (s *PermissionService) CanJoinVoiceChannel(ctx context.Context, workspaceID string, channelID string, userID string) (bool, error) {
	if workspaceID == "" || channelID == "" || userID == "" {
		return false, ErrInvalidQuery
	}

	snapshot, err := s.readRepo.GetPermissionSnapshot(ctx, workspaceID, channelID)
	if err != nil {
		return false, err
	}

	return s.evaluator.CanJoinVoiceChannel(snapshot, userID)
}
