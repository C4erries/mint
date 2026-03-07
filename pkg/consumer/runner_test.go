package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testMessage struct {
	id int
}

type scriptedReader struct {
	messages []testMessage
	acked    []testMessage
	ackErr   error
}

func (r *scriptedReader) Poll(_ context.Context) (testMessage, error) {
	if len(r.messages) == 0 {
		return testMessage{}, context.Canceled
	}

	message := r.messages[0]
	r.messages = r.messages[1:]

	return message, nil
}

func (r *scriptedReader) Ack(_ context.Context, message testMessage) error {
	r.acked = append(r.acked, message)
	return r.ackErr
}

func TestRunner_AckAfterSuccessfulDispatch(t *testing.T) {
	t.Parallel()

	reader := &scriptedReader{messages: []testMessage{{id: 1}}}
	runner := NewRunner[testMessage, string](
		reader,
		func(context.Context, testMessage) error { return nil },
		func(_ testMessage, _ error, _ int, _ time.Time) string { return "" },
		func(context.Context, string) error { return nil },
		Options{Name: "test"},
	)

	err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
}

func TestRunner_AckAfterDLQPublish(t *testing.T) {
	t.Parallel()

	reader := &scriptedReader{messages: []testMessage{{id: 1}}}

	type deadLetter struct {
		Attempts int
	}

	var published []deadLetter
	runner := NewRunner(
		reader,
		func(context.Context, testMessage) error { return errors.New("dispatch failed") },
		func(_ testMessage, _ error, attempts int, _ time.Time) deadLetter {
			return deadLetter{Attempts: attempts}
		},
		func(_ context.Context, message deadLetter) error {
			published = append(published, message)
			return nil
		},
		Options{Name: "test", MaxDispatchAttempts: 2, RetryBackoff: time.Millisecond},
	)

	err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
	require.Len(t, published, 1)
	require.Equal(t, 2, published[0].Attempts)
}

func TestRunner_NoAckWhenDLQPublishFails(t *testing.T) {
	t.Parallel()

	reader := &scriptedReader{messages: []testMessage{{id: 1}}}

	runner := NewRunner(
		reader,
		func(context.Context, testMessage) error { return errors.New("dispatch failed") },
		func(_ testMessage, _ error, _ int, _ time.Time) string { return "dlq" },
		func(context.Context, string) error { return errors.New("dlq unavailable") },
		Options{Name: "test", MaxDispatchAttempts: 2, RetryBackoff: time.Millisecond},
	)

	err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, reader.acked)
}

func TestRunner_UsesConfiguredAttempts(t *testing.T) {
	t.Parallel()

	reader := &scriptedReader{messages: []testMessage{{id: 1}}}

	attempts := 0
	runner := NewRunner(
		reader,
		func(context.Context, testMessage) error {
			attempts++
			return errors.New("dispatch failed")
		},
		func(_ testMessage, _ error, _ int, _ time.Time) string { return "dlq" },
		func(context.Context, string) error { return nil },
		Options{Name: "test", MaxDispatchAttempts: 3, RetryBackoff: time.Millisecond},
	)

	err := runner.Run(context.Background())
	require.NoError(t, err)
	require.Equal(t, 3, attempts)
}
