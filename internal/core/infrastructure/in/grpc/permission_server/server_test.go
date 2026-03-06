package permissionserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	permissionv1 "github.com/c4erries/mint/api/permission/v1"
	workspaceapp "github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
)

type fakeReadRepository struct {
	snapshot domain.PermissionSnapshot
	err      error
}

func (r *fakeReadRepository) GetWorkspace(_ context.Context, _ string) (workspaceapp.WorkspaceView, error) {
	return workspaceapp.WorkspaceView{}, nil
}

func (r *fakeReadRepository) GetChannel(_ context.Context, _ string, _ string) (workspaceapp.ChannelView, error) {
	return workspaceapp.ChannelView{}, nil
}

func (r *fakeReadRepository) GetPermissionSnapshot(_ context.Context, _ string, _ string) (domain.PermissionSnapshot, error) {
	if r.err != nil {
		return domain.PermissionSnapshot{}, r.err
	}

	return r.snapshot, nil
}

func TestServer_CanJoinVoiceChannel(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	baseSnapshot := domain.PermissionSnapshot{
		WorkspaceID: "ws-1",
		ChannelID:   "ch-1",
		Members: map[string]domain.Member{
			"user-1": {
				WorkspaceID: "ws-1",
				UserID:      "user-1",
				JoinedAt:    now,
				Banned:      false,
			},
		},
		RolesByID: map[string]domain.Role{},
	}

	testCases := []struct {
		name          string
		request       *permissionv1.CanJoinVoiceChannelRequest
		repoErr       error
		expectedCode  codes.Code
		expectedAllow bool
	}{
		{
			name:         "invalid request",
			request:      &permissionv1.CanJoinVoiceChannelRequest{WorkspaceId: "", ChannelId: "ch-1", UserId: "user-1"},
			expectedCode: codes.InvalidArgument,
		},
		{
			name:         "workspace not found",
			request:      &permissionv1.CanJoinVoiceChannelRequest{WorkspaceId: "ws-1", ChannelId: "ch-1", UserId: "user-1"},
			repoErr:      domain.ErrWorkspaceNotFound,
			expectedCode: codes.NotFound,
		},
		{
			name:         "internal error",
			request:      &permissionv1.CanJoinVoiceChannelRequest{WorkspaceId: "ws-1", ChannelId: "ch-1", UserId: "user-1"},
			repoErr:      errors.New("boom"),
			expectedCode: codes.Internal,
		},
		{
			name:          "allowed member",
			request:       &permissionv1.CanJoinVoiceChannelRequest{WorkspaceId: "ws-1", ChannelId: "ch-1", UserId: "user-1"},
			expectedCode:  codes.OK,
			expectedAllow: true,
		},
		{
			name:          "denied non member",
			request:       &permissionv1.CanJoinVoiceChannelRequest{WorkspaceId: "ws-1", ChannelId: "ch-1", UserId: "missing"},
			expectedCode:  codes.OK,
			expectedAllow: false,
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			repo := &fakeReadRepository{snapshot: baseSnapshot, err: testCase.repoErr}
			permissionService, err := workspaceapp.NewPermissionService(repo, workspaceapp.NewBaselinePermissionEvaluator())
			require.NoError(t, err)

			server := New(permissionService)
			response, callErr := server.CanJoinVoiceChannel(context.Background(), testCase.request)

			if testCase.expectedCode != codes.OK {
				require.Error(t, callErr)
				grpcStatus, ok := status.FromError(callErr)
				require.True(t, ok)
				require.Equal(t, testCase.expectedCode, grpcStatus.Code())
				return
			}

			require.NoError(t, callErr)
			require.NotNil(t, response)
			require.Equal(t, testCase.expectedAllow, response.GetAllowed())
		})
	}
}
