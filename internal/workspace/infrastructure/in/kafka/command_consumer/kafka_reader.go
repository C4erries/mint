package commandconsumer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultMinBytes = 1
	defaultMaxBytes = 10e6
)

// KafkaReaderConfig defines kafka-go reader parameters used by workspace command consumer.
type KafkaReaderConfig struct {
	Brokers        []string
	Topic          string
	GroupID        string
	MinBytes       int
	MaxBytes       int
	StartOffset    int64
	CommitInterval time.Duration
}

// KafkaMessageReader is a narrow interface over kafka-go reader for easier testing.
type KafkaMessageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaReaderFactory wraps kafka-go constructor so tests can inject fake readers.
type KafkaReaderFactory interface {
	NewReader(config kafka.ReaderConfig) KafkaMessageReader
}

type defaultKafkaReaderFactory struct{}

func (defaultKafkaReaderFactory) NewReader(config kafka.ReaderConfig) KafkaMessageReader {
	return kafka.NewReader(config)
}

// KafkaReader adapts kafka-go Reader to commandconsumer.Reader.
type KafkaReader struct {
	reader KafkaMessageReader
}

func NewKafkaReader(cfg KafkaReaderConfig, factory KafkaReaderFactory) (*KafkaReader, error) {
	brokers := trimAndFilter(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}

	topic := strings.TrimSpace(cfg.Topic)
	if topic == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}

	groupID := strings.TrimSpace(cfg.GroupID)
	if groupID == "" {
		return nil, fmt.Errorf("kafka group id is required")
	}

	if factory == nil {
		factory = defaultKafkaReaderFactory{}
	}

	minBytes := cfg.MinBytes
	if minBytes <= 0 {
		minBytes = defaultMinBytes
	}

	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}

	startOffset := cfg.StartOffset
	if startOffset == 0 {
		startOffset = kafka.FirstOffset
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		StartOffset:    startOffset,
		CommitInterval: cfg.CommitInterval,
	}

	return &KafkaReader{reader: factory.NewReader(readerConfig)}, nil
}

func (r *KafkaReader) Poll(ctx context.Context) (Message, error) {
	message, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}

	return Message{Key: string(message.Key), Value: message.Value, ackToken: message}, nil
}

func (r *KafkaReader) Ack(ctx context.Context, message Message) error {
	token := message.ackToken
	if token == nil {
		return fmt.Errorf("missing kafka ack token")
	}

	rawMessage, ok := token.(kafka.Message)
	if !ok {
		return fmt.Errorf("invalid kafka ack token type %T", token)
	}

	return r.reader.CommitMessages(ctx, rawMessage)
}

func (r *KafkaReader) Close() error {
	if r == nil || r.reader == nil {
		return nil
	}

	return r.reader.Close()
}

func trimAndFilter(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}

		result = append(result, trimmed)
	}

	return result
}
