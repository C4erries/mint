package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	grpcclients "github.com/c4erries/mint/internal/rtc/infrastructure/out/grpc/clients"
	livekitclient "github.com/c4erries/mint/internal/rtc/infrastructure/out/livekit/client"
	"github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/inmemory"
)

func TestCommandService_HandleJoinAndIssueToken(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ctx := context.Background()

	store := inmemory.NewRoomStore()
	grants := inmemory.NewGrantStore(func() time.Time { return now })
	permissions := grpcclients.NewPermissionClient()
	livekit := livekitclient.NewTokenClient("test-key", "test-secret", func() time.Time { return now })

	service, err := application.NewCommandService(
		store,
		grants,
		permissions,
		livekit,
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
		},
	)
	if err != nil {
		t.Fatalf("create command service failed: %v", err)
	}

	queries := application.NewQueryService(store, grants)

	testCases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "join command is idempotent by command id",
			run: func(t *testing.T) {
				joinMeta := application.CommandMeta{
					CommandID:     "cmd-join-1",
					CorrelationID: "corr-1",
					CausationID:   "cause-1",
					MessageID:     "msg-1",
					OccurredAt:    now,
					WorkspaceID:   "ws-1",
					ChannelID:     "ch-1",
					ActorID:       "user-1",
					SchemaVersion: 1,
				}

				joinCommand := application.JoinVoiceChannelCommand{Meta: joinMeta, UserID: "user-1"}
				if runErr := service.JoinVoiceChannel(ctx, joinCommand); runErr != nil {
					t.Fatalf("first join failed: %v", runErr)
				}

				if runErr := service.JoinVoiceChannel(ctx, joinCommand); runErr != nil {
					t.Fatalf("second join should be idempotent, got error: %v", runErr)
				}

				state, runErr := queries.GetVoiceRoomState(ctx, application.GetVoiceRoomStateQuery{WorkspaceID: "ws-1", ChannelID: "ch-1"})
				if runErr != nil {
					t.Fatalf("get room state failed: %v", runErr)
				}

				if state.ParticipantCount != 1 {
					t.Fatalf("expected exactly one active participant, got %d", state.ParticipantCount)
				}

				messages, runErr := store.ListUnpublished(ctx, 100)
				if runErr != nil {
					t.Fatalf("list unpublished outbox failed: %v", runErr)
				}

				joinEvents := 0
				for _, message := range messages {
					if message.EventType == domain.EventVoiceChannelJoined {
						joinEvents++
					}
				}

				if joinEvents != 1 {
					t.Fatalf("expected one join event, got %d", joinEvents)
				}
			},
		},
		{
			name: "issue token stores retrievable grant",
			run: func(t *testing.T) {
				issueMeta := application.CommandMeta{
					CommandID:     "cmd-token-1",
					CorrelationID: "corr-2",
					CausationID:   "cause-2",
					MessageID:     "msg-2",
					OccurredAt:    now.Add(1 * time.Second),
					WorkspaceID:   "ws-1",
					ChannelID:     "ch-1",
					ActorID:       "user-1",
					SchemaVersion: 1,
				}

				if runErr := service.IssueRtcToken(ctx, application.IssueRtcTokenCommand{
					Meta:         issueMeta,
					UserID:       "user-1",
					TTL:          3 * time.Minute,
					CanPublish:   true,
					CanSubscribe: true,
				}); runErr != nil {
					t.Fatalf("issue token failed: %v", runErr)
				}

				messages, runErr := store.ListUnpublished(ctx, 100)
				if runErr != nil {
					t.Fatalf("list unpublished outbox failed: %v", runErr)
				}

				var tokenID string
				for _, message := range messages {
					if message.EventType != domain.EventRtcTokenIssued {
						continue
					}

					tokenID = message.Payload["token_id"]
				}

				if tokenID == "" {
					t.Fatalf("expected token event with token_id payload")
				}

				status, runErr := queries.GetRtcTokenGrantStatus(ctx, application.GetRtcTokenGrantStatusQuery{TokenID: tokenID})
				if runErr != nil {
					t.Fatalf("query token status failed: %v", runErr)
				}

				if status.Expired {
					t.Fatalf("token must not be expired")
				}

				if status.TokenID != tokenID {
					t.Fatalf("expected token id %s, got %s", tokenID, status.TokenID)
				}
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			testCase.run(t)
		})
	}
}

func TestCommandService_PermissionDenied(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	ctx := context.Background()

	store := inmemory.NewRoomStore()
	grants := inmemory.NewGrantStore(func() time.Time { return now })
	permissions := grpcclients.NewPermissionClient()
	permissions.Deny("ws-deny", "ch-deny", "user-deny")
	livekit := livekitclient.NewTokenClient("test-key", "test-secret", func() time.Time { return now })

	service, err := application.NewCommandService(
		store,
		grants,
		permissions,
		livekit,
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
		},
	)
	if err != nil {
		t.Fatalf("create command service failed: %v", err)
	}

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
	if !errors.Is(err, application.ErrPermissionDenied) {
		t.Fatalf("expected permission denied, got %v", err)
	}
}
