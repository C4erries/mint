package eventpublisher

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/workspace/application"
	commandconsumer "github.com/c4erries/mint/internal/workspace/infrastructure/in/kafka/command_consumer"
)

type capturingProducer struct {
	topic string
	key   string
	value []byte
	err   error
}

func (p *capturingProducer) Publish(_ context.Context, topic string, key string, value []byte) error {
	if p.err != nil {
		return p.err
	}

	p.topic = topic
	p.key = key
	p.value = append([]byte(nil), value...)

	return nil
}

func TestNewCommandDLQPublisher_Validation(t *testing.T) {
	t.Parallel()

	_, err := NewCommandDLQPublisher(nil, "topic")
	require.Error(t, err)
	require.ErrorContains(t, err, "kafka producer is required")

	_, err = NewCommandDLQPublisher(&capturingProducer{}, " ")
	require.Error(t, err)
	require.ErrorContains(t, err, "workspace command dlq topic is required")
}

func TestCommandDLQPublisher_Publish(t *testing.T) {
	t.Parallel()

	producer := &capturingProducer{}
	publisher, err := NewCommandDLQPublisher(producer, "workspace.dlq")
	require.NoError(t, err)

	now := time.Now().UTC()
	message := &commandconsumer.DeadLetterMessage{
		FailedAt:    now,
		Attempts:    2,
		Reason:      "decode failed",
		OriginalKey: "ws-1:user-1",
		CommandType: "JoinWorkspace",
		CommandMeta: &application.CommandMeta{CommandID: "cmd-1"},
	}

	err = publisher.Publish(context.Background(), message)
	require.NoError(t, err)
	require.Equal(t, "workspace.dlq", producer.topic)
	require.Equal(t, "ws-1:user-1", producer.key)

	var decoded commandconsumer.DeadLetterMessage
	err = json.Unmarshal(producer.value, &decoded)
	require.NoError(t, err)
	require.Equal(t, message.Attempts, decoded.Attempts)
	require.Equal(t, message.Reason, decoded.Reason)
}

func TestCommandDLQPublisher_PublishValidationAndProducerError(t *testing.T) {
	t.Parallel()

	producer := &capturingProducer{err: errors.New("kafka down")}
	publisher, err := NewCommandDLQPublisher(producer, "workspace.dlq")
	require.NoError(t, err)

	err = publisher.Publish(context.Background(), nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "workspace command dead letter message is required")

	err = publisher.Publish(context.Background(), &commandconsumer.DeadLetterMessage{
		CommandMeta: &application.CommandMeta{CommandID: "cmd-1"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "publish workspace command dead letter message")
}
