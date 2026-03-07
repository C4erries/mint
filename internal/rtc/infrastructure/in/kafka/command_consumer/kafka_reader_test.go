package commandconsumer

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

type fakeKafkaReader struct {
	fetchMessage kafka.Message
	fetchErr     error
	commitErr    error
	committed    []kafka.Message
}

func (f *fakeKafkaReader) FetchMessage(_ context.Context) (kafka.Message, error) {
	if f.fetchErr != nil {
		return kafka.Message{}, f.fetchErr
	}

	return f.fetchMessage, nil
}

func (f *fakeKafkaReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	f.committed = append(f.committed, messages...)
	return f.commitErr
}

func (f *fakeKafkaReader) Close() error {
	return nil
}

type fakeKafkaReaderFactory struct {
	reader    KafkaMessageReader
	lastConfg kafka.ReaderConfig
}

func (f *fakeKafkaReaderFactory) NewReader(config kafka.ReaderConfig) KafkaMessageReader {
	f.lastConfg = config
	return f.reader
}

func TestKafkaReaderPollAndAck(t *testing.T) {
	t.Parallel()

	rawMessage := kafka.Message{Key: []byte("key-1"), Value: []byte("value-1")}
	fakeReader := &fakeKafkaReader{fetchMessage: rawMessage}

	reader := &KafkaReader{reader: fakeReader}

	message, err := reader.Poll(context.Background())
	require.NoError(t, err)
	require.Equal(t, "key-1", message.Key)
	require.Equal(t, []byte("value-1"), message.Value)

	err = reader.Ack(context.Background(), message)
	require.NoError(t, err)
	require.Len(t, fakeReader.committed, 1)
	require.Equal(t, rawMessage, fakeReader.committed[0])
}

func TestKafkaReaderAckWithoutToken(t *testing.T) {
	t.Parallel()

	reader := &KafkaReader{reader: &fakeKafkaReader{}}

	err := reader.Ack(context.Background(), Message{})
	require.ErrorContains(t, err, "missing kafka ack token")
}

func TestNewKafkaReaderRejectsEmptyBrokersAfterTrim(t *testing.T) {
	t.Parallel()

	_, err := NewKafkaReader(KafkaReaderConfig{
		Brokers: []string{" ", "\t"},
		Topic:   "topic-1",
		GroupID: "group-1",
	}, &fakeKafkaReaderFactory{reader: &fakeKafkaReader{}})
	require.ErrorContains(t, err, "kafka brokers are required")
}

func TestNewKafkaReaderTrimsBrokersInConfig(t *testing.T) {
	t.Parallel()

	factory := &fakeKafkaReaderFactory{reader: &fakeKafkaReader{}}

	reader, err := NewKafkaReader(KafkaReaderConfig{
		Brokers: []string{" localhost:9092 ", "", "kafka:9092"},
		Topic:   "topic-1",
		GroupID: "group-1",
	}, factory)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.Equal(t, []string{"localhost:9092", "kafka:9092"}, factory.lastConfg.Brokers)
}
