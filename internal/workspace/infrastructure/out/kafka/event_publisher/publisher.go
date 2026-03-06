package eventpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/c4erries/mint/internal/workspace/application"
)

// KafkaProducer is a thin abstraction over kafka publish operation.
type KafkaProducer interface {
	Publish(ctx context.Context, topic string, key string, value []byte) error
}

// Publisher converts workspace outbox messages into transport payload.
type Publisher struct {
	producer KafkaProducer
	topic    string
}

func NewPublisher(producer KafkaProducer, topic string) *Publisher {
	return &Publisher{producer: producer, topic: topic}
}

func (p *Publisher) Publish(ctx context.Context, message application.OutboxMessage) error {
	if p == nil || p.producer == nil {
		return fmt.Errorf("workspace kafka producer is not configured")
	}

	topic := strings.TrimSpace(p.topic)
	if topic == "" {
		return fmt.Errorf("workspace kafka topic is required")
	}

	rawMessage, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal workspace outbox message: %w", err)
	}

	partitionKey := message.WorkspaceID
	if message.ChannelID != "" {
		partitionKey = message.WorkspaceID + ":" + message.ChannelID
	}

	if err = p.producer.Publish(ctx, topic, partitionKey, rawMessage); err != nil {
		return fmt.Errorf("publish workspace outbox message to kafka: %w", err)
	}

	return nil
}
