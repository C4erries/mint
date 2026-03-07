package commandconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
	consumerrunner "github.com/c4erries/mint/pkg/consumer"
)

// Message is a minimal kafka message abstraction.
type Message struct {
	Key      string
	Value    []byte
	ackToken any
}

// Reader abstracts incoming workspace command source.
type Reader interface {
	Poll(ctx context.Context) (Message, error)
	Ack(ctx context.Context, message Message) error
	Close() error
}

// Consumer dispatches workspace commands to application layer.
type Consumer struct {
	commands *application.CommandService
	runner   *consumerrunner.Runner[Message, *DeadLetterMessage]
}

func New(
	reader Reader,
	commands *application.CommandService,
	deadLetters DeadLetterPublisher,
	logger *slog.Logger,
	options ConsumerOptions,
) *Consumer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	consumerOptions := options.withDefaults()
	consumer := &Consumer{
		commands: commands,
	}

	consumer.runner = consumerrunner.NewRunner(
		reader,
		consumer.dispatch,
		func(message Message, dispatchErr error, attempts int, failedAt time.Time) *DeadLetterMessage {
			return buildDeadLetterMessage(message, dispatchErr, attempts, failedAt)
		},
		func(ctx context.Context, message *DeadLetterMessage) error {
			if deadLetters == nil {
				return fmt.Errorf("workspace dead letter publisher is not configured")
			}

			return deadLetters.Publish(ctx, message)
		},
		consumerrunner.Options{
			MaxDispatchAttempts: consumerOptions.MaxDispatchAttempts,
			RetryBackoff:        consumerOptions.RetryBackoff,
			Now:                 consumerOptions.Now,
			Logger:              logger,
			Name:                "workspace command",
		},
	)

	return consumer
}

func (c *Consumer) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if c.runner == nil {
		return fmt.Errorf("workspace command consumer runner is not configured")
	}

	return c.runner.Run(ctx)
}

type envelope struct {
	Type    string                  `json:"type"`
	Meta    application.CommandMeta `json:"meta"`
	Payload json.RawMessage         `json:"payload"`
}

type createWorkspacePayload struct {
	WorkspaceName string `json:"workspace_name"`
}

type createChannelPayload struct {
	ChannelName string `json:"channel_name"`
	ChannelKind string `json:"channel_kind"`
}

type userPayload struct {
	UserID string `json:"user_id"`
}

func (c *Consumer) dispatch(ctx context.Context, message Message) error {
	if c.commands == nil {
		return fmt.Errorf("workspace command service is not configured")
	}

	var commandEnvelope envelope
	if err := json.Unmarshal(message.Value, &commandEnvelope); err != nil {
		return fmt.Errorf("decode workspace command envelope: %w", err)
	}

	if !application.IsCommandTypeSupported(commandEnvelope.Type) {
		return fmt.Errorf("unsupported workspace command type %q", commandEnvelope.Type)
	}

	switch commandEnvelope.Type {
	case domain.CommandCreateWorkspace:
		var payload createWorkspacePayload
		if err := json.Unmarshal(commandEnvelope.Payload, &payload); err != nil {
			return fmt.Errorf("decode create workspace payload: %w", err)
		}

		return c.commands.CreateWorkspace(ctx, application.CreateWorkspaceCommand{Meta: commandEnvelope.Meta, WorkspaceName: payload.WorkspaceName})
	case domain.CommandCreateChannel:
		var payload createChannelPayload
		if err := json.Unmarshal(commandEnvelope.Payload, &payload); err != nil {
			return fmt.Errorf("decode create channel payload: %w", err)
		}

		return c.commands.CreateChannel(ctx, application.CreateChannelCommand{Meta: commandEnvelope.Meta, ChannelName: payload.ChannelName, ChannelKind: payload.ChannelKind})
	case domain.CommandJoinWorkspace:
		var payload userPayload
		if err := json.Unmarshal(commandEnvelope.Payload, &payload); err != nil {
			return fmt.Errorf("decode join workspace payload: %w", err)
		}

		return c.commands.JoinWorkspace(ctx, application.JoinWorkspaceCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandBanMember:
		var payload userPayload
		if err := json.Unmarshal(commandEnvelope.Payload, &payload); err != nil {
			return fmt.Errorf("decode ban member payload: %w", err)
		}

		return c.commands.BanMember(ctx, application.BanMemberCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	default:
		return fmt.Errorf("workspace command %q is not implemented", commandEnvelope.Type)
	}
}
