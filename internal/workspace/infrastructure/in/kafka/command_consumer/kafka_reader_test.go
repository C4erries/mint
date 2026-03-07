package commandconsumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type fakeKafkaReader struct {
	message      kafka.Message
	fetchErr     error
	commitErr    error
	committed    []kafka.Message
	closedCalled bool
}

func (r *fakeKafkaReader) FetchMessage(_ context.Context) (kafka.Message, error) {
	if r.fetchErr != nil {
		return kafka.Message{}, r.fetchErr
	}

	return r.message, nil
}

func (r *fakeKafkaReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	if r.commitErr != nil {
		return r.commitErr
	}

	r.committed = append(r.committed, messages...)

	return nil
}

func (r *fakeKafkaReader) Close() error {
	r.closedCalled = true

	return nil
}

type fakeKafkaReaderFactory struct {
	reader KafkaMessageReader
	config kafka.ReaderConfig
}

func (f *fakeKafkaReaderFactory) NewReader(config kafka.ReaderConfig) KafkaMessageReader {
	f.config = config

	return f.reader
}

func TestNewKafkaReader_ValidatesConfig(t *testing.T) {
	t.Parallel()

	_, err := NewKafkaReader(KafkaReaderConfig{Topic: "topic", GroupID: "group"}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "kafka brokers are required")

	_, err = NewKafkaReader(KafkaReaderConfig{Brokers: []string{"broker:9092"}, GroupID: "group"}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "kafka topic is required")

	_, err = NewKafkaReader(KafkaReaderConfig{Brokers: []string{"broker:9092"}, Topic: "topic"}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "kafka group id is required")
}

func TestNewKafkaReader_DefaultsAndTrim(t *testing.T) {
	t.Parallel()

	fakeReader := &fakeKafkaReader{}
	factory := &fakeKafkaReaderFactory{reader: fakeReader}

	reader, err := NewKafkaReader(KafkaReaderConfig{
		Brokers: []string{" broker-a:9092 ", "", "broker-b:9092"},
		Topic:   "topic",
		GroupID: "group",
	}, factory)
	require.NoError(t, err)
	require.NotNil(t, reader)

	require.Equal(t, []string{"broker-a:9092", "broker-b:9092"}, factory.config.Brokers)
	require.Equal(t, kafka.FirstOffset, factory.config.StartOffset)
	require.Equal(t, defaultMinBytes, factory.config.MinBytes)
	require.Equal(t, int(defaultMaxBytes), factory.config.MaxBytes)
}

func TestKafkaReader_PollAndAck(t *testing.T) {
	t.Parallel()

	fakeReader := &fakeKafkaReader{
		message: kafka.Message{
			Key:   []byte("k1"),
			Value: []byte("payload"),
			Time:  time.Now(),
		},
	}

	reader := &KafkaReader{reader: fakeReader}
	message, err := reader.Poll(context.Background())
	require.NoError(t, err)
	require.Equal(t, "k1", message.Key)
	require.Equal(t, []byte("payload"), message.Value)

	err = reader.Ack(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, fakeReader.committed, 1)
}

func TestKafkaReader_AckValidation(t *testing.T) {
	t.Parallel()

	reader := &KafkaReader{reader: &fakeKafkaReader{}}

	err := reader.Ack(context.Background(), Message{})
	require.Error(t, err)
	require.ErrorContains(t, err, "missing kafka ack token")

	err = reader.Ack(context.Background(), Message{ackToken: "wrong"})
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid kafka ack token type")
}

func TestKafkaReader_AckError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("commit failed")
	fakeReader := &fakeKafkaReader{
		message:   kafka.Message{Key: []byte("k1"), Value: []byte("payload")},
		commitErr: expectedErr,
	}
	reader := &KafkaReader{reader: fakeReader}

	message, err := reader.Poll(context.Background())
	require.NoError(t, err)

	err = reader.Ack(context.Background(), message)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}
