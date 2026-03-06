package eventpublisher

import (
	"context"
	"fmt"
	"net"
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

// KafkaWriter is a narrow abstraction over kafka-go writer for easier testing.
type KafkaWriter interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

// KafkaWriterFactory wraps kafka-go writer constructor.
type KafkaWriterFactory interface {
	NewWriter(config kafka.WriterConfig) KafkaWriter
}

type defaultKafkaWriterFactory struct{}

func (defaultKafkaWriterFactory) NewWriter(config kafka.WriterConfig) KafkaWriter {
	return kafka.NewWriter(config)
}

// KafkaGoProducerConfig configures kafka-go producer behavior.
type KafkaGoProducerConfig struct {
	Brokers      []string
	BatchTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// KafkaGoProducer publishes events via kafka-go writer.
type KafkaGoProducer struct {
	writer  KafkaWriter
	brokers []string
	mu      sync.Mutex
	closed  bool
}

func NewKafkaGoProducer(cfg KafkaGoProducerConfig, factory KafkaWriterFactory) (*KafkaGoProducer, error) {
	brokers := normalizeBrokers(cfg.Brokers)
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}

	if factory == nil {
		factory = defaultKafkaWriterFactory{}
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

	return &KafkaGoProducer{
		writer:  factory.NewWriter(writerConfig),
		brokers: append([]string(nil), brokers...),
	}, nil
}

func (p *KafkaGoProducer) Publish(ctx context.Context, topic string, key string, value []byte) error {
	topic = strings.TrimSpace(topic)
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

func (p *KafkaGoProducer) Close() error {
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

func (p *KafkaGoProducer) Ping(ctx context.Context) error {
	if p == nil {
		return fmt.Errorf("kafka producer is not initialized")
	}

	if len(p.brokers) == 0 {
		return fmt.Errorf("kafka brokers are not configured")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka broker %s: %w", p.brokers[0], err)
	}

	return connection.Close()
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
