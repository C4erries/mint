package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/rtc/application"
	appmocks "github.com/c4erries/mint/internal/rtc/application/mocks"
	"github.com/c4erries/mint/internal/rtc/domain"
)

func TestCommandService_JoinVoiceChannel_ProcessedCommandIsNoop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	rooms := appmocks.NewVoiceRoomWriteRepository(t)
	tx := appmocks.NewVoiceRoomWriteTx(t)
	grants := appmocks.NewMediaAccessGrantRepository(t)
	permissions := appmocks.NewPermissionChecker(t)
	livekit := appmocks.NewLiveKitClient(t)

	permissions.EXPECT().CanJoinVoiceChannel(ctx, "ws-1", "ch-1", "user-1").Return(true, nil).Once()
	rooms.EXPECT().WithTx(ctx, mock.Anything).
		RunAndReturn(func(callCtx context.Context, fn func(application.VoiceRoomWriteTx) error) error {
			return fn(tx)
		}).
		Once()
	tx.EXPECT().IsCommandProcessed(ctx, "cmd-join").Return(true, nil).Once()

	service, err := application.NewCommandService(
		rooms,
		grants,
		permissions,
		livekit,
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
		},
	)
	require.NoError(t, err)

	err = service.JoinVoiceChannel(ctx, application.JoinVoiceChannelCommand{
		Meta: application.CommandMeta{
			CommandID:     "cmd-join",
			CorrelationID: "corr-join",
			CausationID:   "cause-join",
			MessageID:     "msg-join",
			OccurredAt:    now,
			WorkspaceID:   "ws-1",
			ChannelID:     "ch-1",
			ActorID:       "user-1",
			SchemaVersion: 1,
		},
		UserID: "user-1",
	})
	require.NoError(t, err)
}

func TestCommandService_IssueRtcToken_SavesGrantAndOutbox(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	rooms := appmocks.NewVoiceRoomWriteRepository(t)
	tx := appmocks.NewVoiceRoomWriteTx(t)
	grants := appmocks.NewMediaAccessGrantRepository(t)
	permissions := appmocks.NewPermissionChecker(t)
	livekit := appmocks.NewLiveKitClient(t)

	room, err := domain.NewVoiceRoom("room-1", "ws-1", "ch-1", now)
	require.NoError(t, err)
	_, err = room.JoinParticipant("user-1", now)
	require.NoError(t, err)

	permissions.EXPECT().CanJoinVoiceChannel(ctx, "ws-1", "ch-1", "user-1").Return(true, nil).Once()
	rooms.EXPECT().WithTx(ctx, mock.Anything).
		RunAndReturn(func(callCtx context.Context, fn func(application.VoiceRoomWriteTx) error) error {
			return fn(tx)
		}).
		Once()

	tx.EXPECT().IsCommandProcessed(ctx, "cmd-token").Return(false, nil).Once()
	tx.EXPECT().GetRoom(ctx, "room-1").Return(room.Clone(), nil).Once()
	tx.EXPECT().SaveRoom(ctx, mock.AnythingOfType("*domain.VoiceRoom")).Return(nil).Once()
	tx.EXPECT().AppendOutbox(ctx, mock.MatchedBy(func(message application.OutboxMessage) bool {
		return message.EventType == domain.EventRtcTokenIssued &&
			message.RoomID == "room-1" &&
			message.EventID == "cmd-token:"+domain.EventRtcTokenIssued
	})).Return(nil).Once()
	tx.EXPECT().MarkCommandProcessed(ctx, "cmd-token").Return(nil).Once()

	grants.EXPECT().GetGrantByCommandID(ctx, "cmd-token").Return(domain.MediaAccessGrant{}, domain.ErrGrantNotFound).Once()

	livekit.EXPECT().IssueToken(ctx, mock.MatchedBy(func(request application.LiveKitTokenRequest) bool {
		return request.RoomID == "room-1" && request.UserID == "user-1" && request.TTL == 2*time.Minute
	})).Return(application.LiveKitIssuedToken{
		TokenID:   "token-1",
		Token:     "jwt",
		IssuedAt:  now,
		ExpiresAt: now.Add(2 * time.Minute),
	}, nil).Once()

	grants.EXPECT().SaveGrant(ctx, mock.Anything).Return(nil).Once()

	service, err := application.NewCommandService(
		rooms,
		grants,
		permissions,
		livekit,
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
			IDGenerator: func() string {
				return "evt-1"
			},
		},
	)
	require.NoError(t, err)

	err = service.IssueRtcToken(ctx, application.IssueRtcTokenCommand{
		Meta: application.CommandMeta{
			CommandID:     "cmd-token",
			CorrelationID: "corr-token",
			CausationID:   "cause-token",
			MessageID:     "msg-token",
			OccurredAt:    now,
			WorkspaceID:   "ws-1",
			ChannelID:     "ch-1",
			RoomID:        "room-1",
			ActorID:       "user-1",
			SchemaVersion: 1,
		},
		UserID:       "user-1",
		TTL:          2 * time.Minute,
		CanPublish:   true,
		CanSubscribe: true,
	})
	require.NoError(t, err)
}

func TestCommandService_IssueRtcToken_ReplayUsesExistingGrant(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	rooms := appmocks.NewVoiceRoomWriteRepository(t)
	tx := appmocks.NewVoiceRoomWriteTx(t)
	grants := appmocks.NewMediaAccessGrantRepository(t)
	permissions := appmocks.NewPermissionChecker(t)
	livekit := appmocks.NewLiveKitClient(t)

	room, err := domain.NewVoiceRoom("room-1", "ws-1", "ch-1", now)
	require.NoError(t, err)
	_, err = room.JoinParticipant("user-1", now)
	require.NoError(t, err)

	existingGrant, err := domain.NewMediaAccessGrant(
		"token-existing",
		"cmd-token",
		"room-1",
		"user-1",
		"jwt-existing",
		true,
		true,
		now,
		now.Add(5*time.Minute),
	)
	require.NoError(t, err)

	permissions.EXPECT().CanJoinVoiceChannel(ctx, "ws-1", "ch-1", "user-1").Return(true, nil).Once()
	rooms.EXPECT().WithTx(ctx, mock.Anything).
		RunAndReturn(func(callCtx context.Context, fn func(application.VoiceRoomWriteTx) error) error {
			return fn(tx)
		}).
		Once()

	tx.EXPECT().IsCommandProcessed(ctx, "cmd-token").Return(false, nil).Once()
	tx.EXPECT().GetRoom(ctx, "room-1").Return(room.Clone(), nil).Once()
	tx.EXPECT().SaveRoom(ctx, mock.AnythingOfType("*domain.VoiceRoom")).Return(nil).Once()
	tx.EXPECT().AppendOutbox(ctx, mock.MatchedBy(func(message application.OutboxMessage) bool {
		return message.EventType == domain.EventRtcTokenIssued &&
			message.RoomID == "room-1" &&
			message.Payload["token_id"] == "token-existing"
	})).Return(nil).Once()
	tx.EXPECT().MarkCommandProcessed(ctx, "cmd-token").Return(nil).Once()

	grants.EXPECT().GetGrantByCommandID(ctx, "cmd-token").Return(existingGrant, nil).Once()

	service, err := application.NewCommandService(
		rooms,
		grants,
		permissions,
		livekit,
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
		},
	)
	require.NoError(t, err)

	err = service.IssueRtcToken(ctx, application.IssueRtcTokenCommand{
		Meta: application.CommandMeta{
			CommandID:     "cmd-token",
			CorrelationID: "corr-token",
			CausationID:   "cause-token",
			MessageID:     "msg-token",
			OccurredAt:    now,
			WorkspaceID:   "ws-1",
			ChannelID:     "ch-1",
			RoomID:        "room-1",
			ActorID:       "user-1",
			SchemaVersion: 1,
		},
		UserID:       "user-1",
		TTL:          2 * time.Minute,
		CanPublish:   true,
		CanSubscribe: true,
	})
	require.NoError(t, err)
}

func TestCommandService_JoinVoiceChannel_PermissionDenied(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC()

	rooms := appmocks.NewVoiceRoomWriteRepository(t)
	grants := appmocks.NewMediaAccessGrantRepository(t)
	permissions := appmocks.NewPermissionChecker(t)
	livekit := appmocks.NewLiveKitClient(t)

	permissions.EXPECT().CanJoinVoiceChannel(ctx, "ws-deny", "ch-deny", "user-deny").Return(false, nil).Once()

	service, err := application.NewCommandService(
		rooms,
		grants,
		permissions,
		livekit,
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
		},
	)
	require.NoError(t, err)

	err = service.JoinVoiceChannel(ctx, application.JoinVoiceChannelCommand{
		Meta: application.CommandMeta{
			CommandID:     "cmd-deny",
			CorrelationID: "corr-deny",
			CausationID:   "cause-deny",
			MessageID:     "msg-deny",
			OccurredAt:    now,
			WorkspaceID:   "ws-deny",
			ChannelID:     "ch-deny",
			ActorID:       "user-deny",
			SchemaVersion: 1,
		},
		UserID: "user-deny",
	})
	require.True(t, errors.Is(err, application.ErrPermissionDenied))
}
