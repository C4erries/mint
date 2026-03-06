package producer

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	defaultBatchTimeout = 200 * time.Millisecond
	defaultWriteTimeout = 10 * time.Second
	defaultReadTimeout  = 10 * time.Second
)

// Writer is a narrow abstraction over kafka-go writer for easier testing.
type Writer interface {
	WriteMessages(ctx context.Context, messages ...kafka.Message) error
	Close() error
}

// WriterFactory wraps kafka-go writer constructor.
type WriterFactory interface {
	NewWriter(config kafka.WriterConfig) Writer
}

type defaultWriterFactory struct{}

func (defaultWriterFactory) NewWriter(config kafka.WriterConfig) Writer {
	return kafka.NewWriter(config)
}

// Config configures kafka-go producer behavior.
type Config struct {
	Brokers      []string
	BatchTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Producer publishes messages via kafka-go writer.
type Producer struct {
	writer Writer
	mu     sync.Mutex
	closed bool
}

func New(cfg Config, factory WriterFactory) (*Producer, error) {
	brokers := normalizeBrokers(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}

	if factory == nil {
		factory = defaultWriterFactory{}
	}

	batchTimeout := cfg.BatchTimeout
	if batchTimeout <= 0 {
		batchTimeout = defaultBatchTimeout
	}

	readTimeout := cfg.ReadTimeout
	if readTimeout <= 0 {
		readTimeout = defaultReadTimeout
	}

	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}

	writerConfig := kafka.WriterConfig{
		Brokers:      brokers,
		Balancer:     &kafka.Hash{},
		BatchTimeout: batchTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		RequiredAcks: int(kafka.RequireAll),
	}

	return &Producer{writer: factory.NewWriter(writerConfig)}, nil
}

func (p *Producer) Publish(ctx context.Context, topic string, key string, value []byte) error {
	if topic == "" {
		return fmt.Errorf("kafka topic is required")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()

	if closed {
		return fmt.Errorf("kafka producer is closed")
	}

	message := kafka.Message{Topic: topic, Key: []byte(key), Value: value}
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		return fmt.Errorf("write kafka message: %w", err)
	}

	return nil
}

func (p *Producer) Close() error {
	if p == nil || p.writer == nil {
		return nil
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}

	p.closed = true
	p.mu.Unlock()

	return p.writer.Close()
}

func normalizeBrokers(values []string) []string {
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
