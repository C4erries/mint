package application

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/workspace/domain"
)

func TestBaselinePermissionEvaluator_CanJoinVoiceChannel(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	evaluator := NewBaselinePermissionEvaluator()

	member := domain.Member{
		WorkspaceID: "ws-1",
		UserID:      "user-1",
		JoinedAt:    now,
		Banned:      false,
	}

	testCases := []struct {
		name     string
		snapshot domain.PermissionSnapshot
		userID   string
		allowed  bool
	}{
		{
			name: "allow active member",
			snapshot: domain.PermissionSnapshot{
				WorkspaceID: "ws-1",
				ChannelID:   "ch-1",
				Members:     map[string]domain.Member{"user-1": member},
				RolesByID:   map[string]domain.Role{},
			},
			userID:  "user-1",
			allowed: true,
		},
		{
			name: "deny non-member",
			snapshot: domain.PermissionSnapshot{
				WorkspaceID: "ws-1",
				ChannelID:   "ch-1",
				Members:     map[string]domain.Member{},
				RolesByID:   map[string]domain.Role{},
			},
			userID:  "user-2",
			allowed: false,
		},
		{
			name: "deny banned member",
			snapshot: domain.PermissionSnapshot{
				WorkspaceID: "ws-1",
				ChannelID:   "ch-1",
				Members: map[string]domain.Member{
					"user-1": {
						WorkspaceID: "ws-1",
						UserID:      "user-1",
						JoinedAt:    now,
						Banned:      true,
					},
				},
				RolesByID: map[string]domain.Role{},
			},
			userID:  "user-1",
			allowed: false,
		},
		{
			name: "deny explicit user override",
			snapshot: domain.PermissionSnapshot{
				WorkspaceID: "ws-1",
				ChannelID:   "ch-1",
				Members:     map[string]domain.Member{"user-1": member},
				RolesByID:   map[string]domain.Role{},
				ChannelOverrides: []domain.ChannelOverride{
					{WorkspaceID: "ws-1", ChannelID: "ch-1", SubjectType: "user", SubjectID: "user-1", DenyMask: domain.JoinVoiceMask, UpdatedAt: now},
				},
			},
			userID:  "user-1",
			allowed: false,
		},
		{
			name: "allow explicit user override",
			snapshot: domain.PermissionSnapshot{
				WorkspaceID: "ws-1",
				ChannelID:   "ch-1",
				Members:     map[string]domain.Member{"user-1": member},
				RolesByID:   map[string]domain.Role{},
				ChannelOverrides: []domain.ChannelOverride{
					{WorkspaceID: "ws-1", ChannelID: "ch-1", SubjectType: "user", SubjectID: "user-1", AllowMask: domain.JoinVoiceMask, UpdatedAt: now},
				},
			},
			userID:  "user-1",
			allowed: true,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			allowed, err := evaluator.CanJoinVoiceChannel(testCase.snapshot, testCase.userID)
			require.NoError(t, err)
			require.Equal(t, testCase.allowed, allowed)
		})
	}
}
