package commandconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/workspace/application"
)

type scriptedReader struct {
	messages []Message
	acked    []Message
	ackErr   error
}

func (r *scriptedReader) Poll(_ context.Context) (Message, error) {
	if len(r.messages) == 0 {
		return Message{}, context.Canceled
	}

	message := r.messages[0]
	r.messages = r.messages[1:]

	return message, nil
}

func (r *scriptedReader) Ack(_ context.Context, message Message) error {
	r.acked = append(r.acked, message)
	return r.ackErr
}

func (r *scriptedReader) Close() error {
	return nil
}

type fakeDeadLetterPublisher struct {
	messages []DeadLetterMessage
	err      error
}

func (p *fakeDeadLetterPublisher) Publish(_ context.Context, message *DeadLetterMessage) error {
	if message != nil {
		p.messages = append(p.messages, *message)
	}

	return p.err
}

func TestConsumerRunAckAfterDLQPublish(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	dlqPublisher := &fakeDeadLetterPublisher{}
	message := Message{Value: mustMarshalEnvelope(t, "unknown-command", defaultMeta(now), map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, nil, dlqPublisher, nil, ConsumerOptions{
		MaxDispatchAttempts: 2,
		RetryBackoff:        time.Millisecond,
		Now:                 func() time.Time { return now },
	})

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
	require.Len(t, dlqPublisher.messages, 1)
	require.Equal(t, 2, dlqPublisher.messages[0].Attempts)
	require.True(t, strings.Contains(dlqPublisher.messages[0].Reason, "workspace command service is not configured"))
}

func TestConsumerRunDoesNotAckWhenDLQPublishFails(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	dlqPublisher := &fakeDeadLetterPublisher{err: errors.New("dlq unavailable")}
	message := Message{Value: mustMarshalEnvelope(t, "unknown-command", defaultMeta(now), map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, nil, dlqPublisher, nil, ConsumerOptions{
		MaxDispatchAttempts: 2,
		RetryBackoff:        time.Millisecond,
	})

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, reader.acked)
	require.Len(t, dlqPublisher.messages, 1)
}

func defaultMeta(occurredAt time.Time) application.CommandMeta {
	return application.CommandMeta{
		CommandID:     "cmd-1",
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		MessageID:     "msg-1",
		OccurredAt:    occurredAt,
		WorkspaceID:   "ws-1",
		ChannelID:     "ch-1",
		ActorID:       "user-1",
		SchemaVersion: 1,
	}
}

func mustMarshalEnvelope(t *testing.T, commandType string, meta application.CommandMeta, payload map[string]any) []byte {
	t.Helper()

	envelope := map[string]any{
		"type":    commandType,
		"meta":    meta,
		"payload": payload,
	}

	encoded, err := json.Marshal(envelope)
	require.NoError(t, err)

	return encoded
}
