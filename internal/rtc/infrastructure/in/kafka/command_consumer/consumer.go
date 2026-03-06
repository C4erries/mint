package commandconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

// Message is a minimal Kafka message abstraction.
type Message struct {
	Key   string
	Value []byte
}

// Reader abstracts source of incoming command messages.
type Reader interface {
	Poll(ctx context.Context) (Message, error)
	Close() error
}

// NoopReader blocks until context cancellation.
type NoopReader struct{}

func (NoopReader) Poll(ctx context.Context) (Message, error) {
	<-ctx.Done()
	return Message{}, ctx.Err()
}

func (NoopReader) Close() error {
	return nil
}

// Consumer parses Kafka envelopes and dispatches RTC commands to application layer.
type Consumer struct {
	reader   Reader
	commands *application.CommandService
	logger   *slog.Logger
}

func New(reader Reader, commands *application.CommandService, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Consumer{
		reader:   reader,
		commands: commands,
		logger:   logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.reader.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}

			c.logger.Error("failed to poll rtc command", slog.String("error", err.Error()))
			continue
		}

		if err = c.dispatch(ctx, message); err != nil {
			c.logger.Error("failed to dispatch rtc command", slog.String("error", err.Error()))
		}
	}
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
