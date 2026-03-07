package producer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type stubWriter struct {
	messages []kafka.Message
	writeErr error
	closed   bool
}

func (w *stubWriter) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	if w.writeErr != nil {
		return w.writeErr
	}

	w.messages = append(w.messages, messages...)

	return nil
}

func (w *stubWriter) Close() error {
	w.closed = true

	return nil
}

type captureFactory struct {
	config kafka.WriterConfig
	writer Writer
}

func (f *captureFactory) NewWriter(config kafka.WriterConfig) Writer {
	f.config = config

	return f.writer
}

func TestNew_RequiresBrokers(t *testing.T) {
	t.Parallel()

	_, err := New(Config{Brokers: []string{"  ", ""}}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "kafka brokers are required")
}

func TestNew_AppliesDefaultsAndTrimsBrokers(t *testing.T) {
	t.Parallel()

	writer := &stubWriter{}
	factory := &captureFactory{writer: writer}

	producer, err := New(Config{Brokers: []string{" broker-1:9092 ", "", "broker-2:9092"}}, factory)
	require.NoError(t, err)
	require.NotNil(t, producer)

	require.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, factory.config.Brokers)
	require.Equal(t, defaultBatchTimeout, factory.config.BatchTimeout)
	require.Equal(t, defaultReadTimeout, factory.config.ReadTimeout)
	require.Equal(t, defaultWriteTimeout, factory.config.WriteTimeout)
}

func TestPublish_WritesMessage(t *testing.T) {
	t.Parallel()

	writer := &stubWriter{}
	producer := &Producer{writer: writer}

	err := producer.Publish(context.Background(), "topic-a", "key-a", []byte("value-a"))
	require.NoError(t, err)
	require.Len(t, writer.messages, 1)
	require.Equal(t, "topic-a", writer.messages[0].Topic)
	require.Equal(t, []byte("key-a"), writer.messages[0].Key)
	require.Equal(t, []byte("value-a"), writer.messages[0].Value)
}

func TestPublish_FailsAfterClose(t *testing.T) {
	t.Parallel()

	writer := &stubWriter{}
	producer := &Producer{writer: writer}

	err := producer.Close()
	require.NoError(t, err)
	require.True(t, writer.closed)

	err = producer.Publish(context.Background(), "topic-a", "key-a", []byte("value-a"))
	require.Error(t, err)
	require.ErrorContains(t, err, "kafka producer is closed")
}

func TestPublish_PropagatesWriterError(t *testing.T) {
	t.Parallel()

	writer := &stubWriter{writeErr: errors.New("write failed")}
	producer := &Producer{writer: writer}

	err := producer.Publish(context.Background(), "topic-a", "key-a", []byte("value-a"))
	require.Error(t, err)
	require.ErrorContains(t, err, "write kafka message")
}

func TestPublish_HonorsCanceledContext(t *testing.T) {
	t.Parallel()

	writer := &stubWriter{}
	producer := &Producer{writer: writer}

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	cancel()

	err := producer.Publish(ctx, "topic-a", "key-a", []byte("value-a"))
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}
