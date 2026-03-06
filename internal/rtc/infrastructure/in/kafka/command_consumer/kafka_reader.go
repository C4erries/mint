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

// KafkaReaderConfig defines kafka-go reader parameters used by rtc command consumer.
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
	ReadMessage(ctx context.Context) (kafka.Message, error)
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
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}

	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}

	if cfg.GroupID == "" {
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
		Brokers:        trimAndFilter(cfg.Brokers),
		GroupID:        cfg.GroupID,
		Topic:          cfg.Topic,
		MinBytes:       minBytes,
		MaxBytes:       maxBytes,
		StartOffset:    startOffset,
		CommitInterval: cfg.CommitInterval,
	}

	return &KafkaReader{reader: factory.NewReader(readerConfig)}, nil
}

func (r *KafkaReader) Poll(ctx context.Context) (Message, error) {
	message, err := r.reader.ReadMessage(ctx)
	if err != nil {
		return Message{}, err
	}

	return Message{Key: string(message.Key), Value: message.Value}, nil
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
