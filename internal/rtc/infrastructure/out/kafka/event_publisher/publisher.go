package eventpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/c4erries/mint/internal/rtc/application"
)

// KafkaProducer is a thin abstraction over Kafka publish operation.
type KafkaProducer interface {
	Publish(ctx context.Context, topic string, key string, value []byte) error
}

// ProducedMessage is useful in tests/local mode to inspect published messages.
type ProducedMessage struct {
	Topic string
	Key   string
	Value []byte
}

// InMemoryProducer stores published messages in process memory.
type InMemoryProducer struct {
	mu       sync.Mutex
	messages []ProducedMessage
}

func NewInMemoryProducer() *InMemoryProducer {
	return &InMemoryProducer{
		messages: make([]ProducedMessage, 0),
	}
}

func (p *InMemoryProducer) Publish(ctx context.Context, topic string, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	copyValue := make([]byte, len(value))
	copy(copyValue, value)

	p.messages = append(p.messages, ProducedMessage{
		Topic: topic,
		Key:   key,
		Value: copyValue,
	})

	return nil
}

func (p *InMemoryProducer) Messages() []ProducedMessage {
	p.mu.Lock()
	defer p.mu.Unlock()

	copied := make([]ProducedMessage, 0, len(p.messages))
	for _, message := range p.messages {
		copyValue := make([]byte, len(message.Value))
		copy(copyValue, message.Value)

		copied = append(copied, ProducedMessage{
			Topic: message.Topic,
			Key:   message.Key,
			Value: copyValue,
		})
	}

	return copied
}

// Publisher converts outbox messages into transport messages.
type Publisher struct {
	producer KafkaProducer
	topic    string
}

func NewPublisher(producer KafkaProducer, topic string) *Publisher {
	return &Publisher{
		producer: producer,
		topic:    topic,
	}
}

func (p *Publisher) Publish(ctx context.Context, message application.OutboxMessage) error {
	rawMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal outbox message: %w", err)
	}

	partitionKey := partitionKeyForMessage(message)
	if err = p.producer.Publish(ctx, p.topic, partitionKey, rawMessage); err != nil {
		return fmt.Errorf("publish outbox message to kafka: %w", err)
	}

	return nil
}

func partitionKeyForMessage(message application.OutboxMessage) string {
	if message.RoomID != "" {
		return message.RoomID
	}

	return message.WorkspaceID + ":" + message.ChannelID
}
