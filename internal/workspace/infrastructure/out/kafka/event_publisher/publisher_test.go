package eventpublisher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/workspace/application"
)

type fakeKafkaProducer struct {
	topic string
	key   string
	value []byte
	err   error
}

func (p *fakeKafkaProducer) Publish(_ context.Context, topic string, key string, value []byte) error {
	if p.err != nil {
		return p.err
	}

	p.topic = topic
	p.key = key

	p.value = append([]byte(nil), value...)

	return nil
}

func TestPublisher_Publish(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	publisher := NewPublisher(producer, "workspace.events")

	message := application.OutboxMessage{
		EventID:     "event-1",
		EventType:   "WorkspaceCreated",
		WorkspaceID: "ws-1",
		ChannelID:   "ch-1",
		OccurredAt:  time.Now().UTC(),
	}

	err := publisher.Publish(context.Background(), message)
	require.NoError(t, err)
	require.Equal(t, "workspace.events", producer.topic)
	require.Equal(t, "ws-1:ch-1", producer.key)

	var decoded application.OutboxMessage

	err = json.Unmarshal(producer.value, &decoded)
	require.NoError(t, err)
	require.Equal(t, message.EventID, decoded.EventID)
}

func TestPublisher_PublishWithoutChannelUsesWorkspaceKey(t *testing.T) {
	t.Parallel()

	producer := &fakeKafkaProducer{}
	publisher := NewPublisher(producer, "workspace.events")

	err := publisher.Publish(context.Background(), application.OutboxMessage{
		EventID:     "event-1",
		EventType:   "WorkspaceCreated",
		WorkspaceID: "ws-42",
		OccurredAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	require.Equal(t, "ws-42", producer.key)
}

func TestPublisher_ValidationAndError(t *testing.T) {
	t.Parallel()

	publisher := NewPublisher(nil, "workspace.events")
	err := publisher.Publish(context.Background(), application.OutboxMessage{})
	require.Error(t, err)
	require.ErrorContains(t, err, "workspace kafka producer is not configured")

	publisher = NewPublisher(&fakeKafkaProducer{}, "  ")
	err = publisher.Publish(context.Background(), application.OutboxMessage{})
	require.Error(t, err)
	require.ErrorContains(t, err, "workspace kafka topic is required")

	producer := &fakeKafkaProducer{err: errors.New("kafka failed")}
	publisher = NewPublisher(producer, "workspace.events")
	err = publisher.Publish(context.Background(), application.OutboxMessage{WorkspaceID: "ws-1", OccurredAt: time.Now()})
	require.Error(t, err)
	require.ErrorContains(t, err, "publish workspace outbox message to kafka")
}
