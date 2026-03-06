package application

import "github.com/c4erries/mint/internal/workspace/domain"

// BaselinePermissionEvaluator is temporary policy implementation with extension seam for full role engine.
type BaselinePermissionEvaluator struct{}

func NewBaselinePermissionEvaluator() *BaselinePermissionEvaluator {
	return &BaselinePermissionEvaluator{}
}

func (e *BaselinePermissionEvaluator) CanJoinVoiceChannel(snapshot domain.PermissionSnapshot, userID string) (bool, error) {
	member, ok := snapshot.Members[userID]
	if !ok {
		return false, nil
	}

	if member.Banned {
		return false, nil
	}

	if snapshot.HasExplicitJoinVoiceDeny(userID) {
		return false, nil
	}

	if snapshot.HasExplicitJoinVoiceAllow(userID) {
		return true, nil
	}

	// Baseline default: active workspace member can join voice.
	return true, nil
}
