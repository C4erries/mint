package commandconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	consumerrunner "github.com/c4erries/mint/pkg/consumer"
)

// Message is a minimal Kafka message abstraction.
type Message struct {
	Key      string
	Value    []byte
	ackToken any
}

// Reader abstracts source of incoming command messages.
type Reader interface {
	Poll(ctx context.Context) (Message, error)
	Ack(ctx context.Context, message Message) error
	Close() error
}

// NoopReader blocks until context cancellation.
type NoopReader struct{}

func (NoopReader) Poll(ctx context.Context) (Message, error) {
	<-ctx.Done()
	return Message{}, ctx.Err()
}

func (NoopReader) Ack(_ context.Context, _ Message) error {
	return nil
}

func (NoopReader) Close() error {
	return nil
}

// Consumer parses Kafka envelopes and dispatches RTC commands to application layer.
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
				return fmt.Errorf("dead letter publisher is not configured")
			}

			return deadLetters.Publish(ctx, message)
		},
		consumerrunner.Options{
			MaxDispatchAttempts: consumerOptions.MaxDispatchAttempts,
			RetryBackoff:        consumerOptions.RetryBackoff,
			Now:                 consumerOptions.Now,
			Logger:              logger,
			Name:                "rtc command",
		},
	)

	return consumer
}

func (c *Consumer) Run(ctx context.Context) error {
	if c == nil {
		return nil
	}

	if c.runner == nil {
		return fmt.Errorf("rtc command consumer runner is not configured")
	}

	return c.runner.Run(ctx)
}

type envelope struct {
	Type    string                  `json:"type"`
	Meta    application.CommandMeta `json:"meta"`
	Payload json.RawMessage         `json:"payload"`
}

type userPayload struct {
	UserID string `json:"user_id"`
}

type issueTokenPayload struct {
	UserID       string `json:"user_id"`
	TTLSeconds   int64  `json:"ttl_seconds"`
	CanPublish   bool   `json:"can_publish"`
	CanSubscribe bool   `json:"can_subscribe"`
}

func (c *Consumer) dispatch(ctx context.Context, message Message) error {
	var commandEnvelope envelope
	if err := json.Unmarshal(message.Value, &commandEnvelope); err != nil {
		return fmt.Errorf("decode command envelope: %w", err)
	}

	if !application.IsCommandTypeSupported(commandEnvelope.Type) {
		return fmt.Errorf("unsupported command type %q", commandEnvelope.Type)
	}

	if c.commands == nil {
		return fmt.Errorf("command service is not configured")
	}

	switch commandEnvelope.Type {
	case domain.CommandJoinVoiceChannel:
		payload, err := decodeUserPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.JoinVoiceChannel(ctx, application.JoinVoiceChannelCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandLeaveVoiceChannel:
		payload, err := decodeUserPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.LeaveVoiceChannel(ctx, application.LeaveVoiceChannelCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandMuteSelf:
		payload, err := decodeUserPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.MuteSelf(ctx, application.MuteSelfCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandUnmuteSelf:
		payload, err := decodeUserPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.UnmuteSelf(ctx, application.UnmuteSelfCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandEnableCamera:
		payload, err := decodeUserPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.EnableCamera(ctx, application.EnableCameraCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandDisableCamera:
		payload, err := decodeUserPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.DisableCamera(ctx, application.DisableCameraCommand{Meta: commandEnvelope.Meta, UserID: payload.UserID})
	case domain.CommandIssueRtcToken:
		payload, err := decodeIssueTokenPayload(commandEnvelope.Payload)
		if err != nil {
			return err
		}

		return c.commands.IssueRtcToken(ctx, application.IssueRtcTokenCommand{
			Meta:         commandEnvelope.Meta,
			UserID:       payload.UserID,
			TTL:          time.Duration(payload.TTLSeconds) * time.Second,
			CanPublish:   payload.CanPublish,
			CanSubscribe: payload.CanSubscribe,
		})
	case domain.CommandTerminateVoiceState:
		return c.commands.TerminateVoiceSession(ctx, application.TerminateVoiceSessionCommand{Meta: commandEnvelope.Meta})
	default:
		return fmt.Errorf("command type %q is not implemented", commandEnvelope.Type)
	}
}

func decodeUserPayload(rawPayload json.RawMessage) (userPayload, error) {
	var payload userPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return userPayload{}, fmt.Errorf("decode user payload: %w", err)
	}

	return payload, nil
}

func decodeIssueTokenPayload(rawPayload json.RawMessage) (issueTokenPayload, error) {
	var payload issueTokenPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		return issueTokenPayload{}, fmt.Errorf("decode issue token payload: %w", err)
	}

	return payload, nil
}
