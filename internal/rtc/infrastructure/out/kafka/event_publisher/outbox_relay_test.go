package eventpublisher

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/rtc/application"
	appmocks "github.com/c4erries/mint/internal/rtc/application/mocks"
)

func TestOutboxRelay_FlushOnce_PublishAndMark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := appmocks.NewOutboxStore(t)
	publisher := appmocks.NewEventPublisher(t)

	messages := []application.OutboxMessage{
		{EventID: "evt-1", EventType: "VoiceChannelJoined", WorkspaceID: "ws", ChannelID: "ch", OccurredAt: time.Now().UTC()},
		{EventID: "evt-2", EventType: "VoiceChannelLeft", WorkspaceID: "ws", ChannelID: "ch", OccurredAt: time.Now().UTC()},
	}

	store.EXPECT().ListUnpublished(ctx, 50).Return(messages, nil).Once()
	publisher.EXPECT().Publish(ctx, messages[0]).Return(nil).Once()
	publisher.EXPECT().Publish(ctx, messages[1]).Return(nil).Once()
	store.EXPECT().MarkPublished(ctx, "evt-1").Return(nil).Once()
	store.EXPECT().MarkPublished(ctx, "evt-2").Return(nil).Once()

	relay, err := NewOutboxRelay(store, publisher, nil, 50)
	require.NoError(t, err)

	err = relay.FlushOnce(ctx)
	require.NoError(t, err)
}

func TestOutboxRelay_FlushOnce_PublishErrorStopsBatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := appmocks.NewOutboxStore(t)
	publisher := appmocks.NewEventPublisher(t)

	message := application.OutboxMessage{EventID: "evt-1", EventType: "VoiceChannelJoined", WorkspaceID: "ws", ChannelID: "ch", OccurredAt: time.Now().UTC()}

	store.EXPECT().ListUnpublished(ctx, 10).Return([]application.OutboxMessage{message}, nil).Once()
	publisher.EXPECT().Publish(ctx, message).Return(assertiveError{}).Once()
	store.EXPECT().MarkPublished(mock.Anything, mock.Anything).Maybe()

	relay, err := NewOutboxRelay(store, publisher, nil, 10)
	require.NoError(t, err)

	err = relay.FlushOnce(ctx)
	require.Error(t, err)
}

type assertiveError struct{}

func (assertiveError) Error() string {
	return "publish failed"
}
