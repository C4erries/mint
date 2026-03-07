package eventpublisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	commandconsumer "github.com/c4erries/mint/internal/workspace/infrastructure/in/kafka/command_consumer"
)

// CommandDLQPublisher publishes poison workspace commands into dedicated DLQ Kafka topic.
type CommandDLQPublisher struct {
	producer KafkaProducer
	topic    string
}

func NewCommandDLQPublisher(producer KafkaProducer, topic string) (*CommandDLQPublisher, error) {
	if producer == nil {
		return nil, fmt.Errorf("kafka producer is required")
	}

	trimmedTopic := strings.TrimSpace(topic)
	if trimmedTopic == "" {
		return nil, fmt.Errorf("workspace command dlq topic is required")
	}

	return &CommandDLQPublisher{producer: producer, topic: trimmedTopic}, nil
}

func (p *CommandDLQPublisher) Publish(ctx context.Context, message *commandconsumer.DeadLetterMessage) error {
	if message == nil {
		return fmt.Errorf("workspace command dead letter message is required")
	}

	payload, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal workspace command dead letter message: %w", err)
	}

	key := message.OriginalKey
	if key == "" && message.CommandMeta != nil {
		key = message.CommandMeta.CommandID
	}

	if err = p.producer.Publish(ctx, p.topic, key, payload); err != nil {
		return fmt.Errorf("publish workspace command dead letter message: %w", err)
	}

	return nil
}
