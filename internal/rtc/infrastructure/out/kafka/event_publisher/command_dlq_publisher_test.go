package eventpublisher

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/rtc/application"
	commandconsumer "github.com/c4erries/mint/internal/rtc/infrastructure/in/kafka/command_consumer"
)

func TestNewCommandDLQPublisher_ValidateInput(t *testing.T) {
	t.Parallel()

	_, err := NewCommandDLQPublisher(nil, "topic")
	require.ErrorContains(t, err, "kafka producer is required")

	_, err = NewCommandDLQPublisher(NewInMemoryProducer(), "")
	require.ErrorContains(t, err, "rtc command dlq topic is required")
}

func TestCommandDLQPublisher_Publish(t *testing.T) {
	t.Parallel()

	producer := NewInMemoryProducer()
	publisher, err := NewCommandDLQPublisher(producer, "mint.rtc.commands.dlq.v1")
	require.NoError(t, err)

	message := commandconsumer.DeadLetterMessage{
		FailedAt:        time.Now().UTC(),
		Attempts:        3,
		Reason:          "unsupported command",
		OriginalPayload: []byte(`{"type":"unknown"}`),
		CommandType:     "unknown",
		CommandMeta: &application.CommandMeta{
			CommandID:     "cmd-1",
			CorrelationID: "corr-1",
			MessageID:     "msg-1",
			OccurredAt:    time.Now().UTC(),
			WorkspaceID:   "ws-1",
			ChannelID:     "ch-1",
			ActorID:       "user-1",
			SchemaVersion: 1,
		},
	}

	err = publisher.Publish(context.Background(), &message)
	require.NoError(t, err)

	produced := producer.Messages()
	require.Len(t, produced, 1)
	require.Equal(t, "mint.rtc.commands.dlq.v1", produced[0].Topic)
	require.Equal(t, "cmd-1", produced[0].Key)

	var decoded commandconsumer.DeadLetterMessage

	err = json.Unmarshal(produced[0].Value, &decoded)
	require.NoError(t, err)
	require.Equal(t, message.Attempts, decoded.Attempts)
	require.Equal(t, message.Reason, decoded.Reason)
	require.Equal(t, message.CommandType, decoded.CommandType)
}
