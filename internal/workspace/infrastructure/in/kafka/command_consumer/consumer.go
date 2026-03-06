package commandconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/c4erries/mint/internal/workspace/application"
	"github.com/c4erries/mint/internal/workspace/domain"
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
	reader   Reader
	commands *application.CommandService
	logger   *slog.Logger
}

func New(reader Reader, commands *application.CommandService, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Consumer{reader: reader, commands: commands, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			c.logger.Error("failed to poll workspace command", slog.String("error", err.Error()))
			continue
		}

		if err = c.dispatch(ctx, message); err != nil {
			c.logger.Error("failed to dispatch workspace command", slog.String("error", err.Error()))
			continue
		}

		if err = c.reader.Ack(ctx, message); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			c.logger.Error("failed to ack workspace command", slog.String("error", err.Error()))
		}
	}
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
