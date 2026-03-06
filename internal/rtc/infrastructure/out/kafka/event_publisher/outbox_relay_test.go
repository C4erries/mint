package eventpublisher

import (
	"context"
	"testing"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	grpcclients "github.com/c4erries/mint/internal/rtc/infrastructure/out/grpc/clients"
	livekitclient "github.com/c4erries/mint/internal/rtc/infrastructure/out/livekit/client"
	"github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/inmemory"
)

func TestOutboxRelayFlushOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Date(2026, time.January, 4, 10, 0, 0, 0, time.UTC)

	store := inmemory.NewRoomStore()
	grants := inmemory.NewGrantStore(func() time.Time { return now })
	permissions := grpcclients.NewPermissionClient()
	livekit := livekitclient.NewTokenClient("test-key", "test-secret", func() time.Time { return now })

	commands, err := application.NewCommandService(
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

	joinErr := commands.JoinVoiceChannel(ctx, application.JoinVoiceChannelCommand{
		Meta: application.CommandMeta{
			CommandID:     "cmd-outbox-join",
			CorrelationID: "corr-outbox",
			CausationID:   "cause-outbox",
			MessageID:     "msg-outbox",
			OccurredAt:    now,
			WorkspaceID:   "ws-outbox",
			ChannelID:     "ch-outbox",
			ActorID:       "user-outbox",
			SchemaVersion: 1,
		},
		UserID: "user-outbox",
	})
	if joinErr != nil {
		t.Fatalf("join command failed: %v", joinErr)
	}

	producer := NewInMemoryProducer()
	publisher := NewPublisher(producer, "mint.rtc.events.v1")
	relay, err := NewOutboxRelay(store, publisher, nil, 50)
	if err != nil {
		t.Fatalf("create outbox relay failed: %v", err)
	}

	if err = relay.FlushOnce(ctx); err != nil {
		t.Fatalf("flush once failed: %v", err)
	}

	published := producer.Messages()
	if len(published) != 1 {
		t.Fatalf("expected one published message, got %d", len(published))
	}

	unpublished, err := store.ListUnpublished(ctx, 100)
	if err != nil {
		t.Fatalf("list unpublished failed: %v", err)
	}

	if len(unpublished) != 0 {
		t.Fatalf("expected no unpublished events after relay flush, got %d", len(unpublished))
	}
}
