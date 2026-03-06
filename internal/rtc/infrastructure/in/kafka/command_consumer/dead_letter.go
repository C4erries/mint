package commandconsumer

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
)

const (
	defaultMaxDispatchAttempts = 3
	defaultRetryBackoff        = 200 * time.Millisecond
)

// ConsumerOptions configures retry and dead-letter behavior for command handling.
type ConsumerOptions struct {
	MaxDispatchAttempts int
	RetryBackoff        time.Duration
	Now                 func() time.Time
}

// DeadLetterPublisher stores poison commands that exceeded retry policy.
type DeadLetterPublisher interface {
	Publish(ctx context.Context, message *DeadLetterMessage) error
}

// DeadLetterMessage captures failed command with metadata for later inspection/replay.
type DeadLetterMessage struct {
	FailedAt        time.Time                `json:"failed_at"`
	Attempts        int                      `json:"attempts"`
	Reason          string                   `json:"reason"`
	OriginalKey     string                   `json:"original_key,omitempty"`
	OriginalPayload json.RawMessage          `json:"original_payload"`
	CommandType     string                   `json:"command_type,omitempty"`
	CommandMeta     *application.CommandMeta `json:"command_meta,omitempty"`
}

func (o ConsumerOptions) withDefaults() ConsumerOptions {
	if o.MaxDispatchAttempts <= 0 {
		o.MaxDispatchAttempts = defaultMaxDispatchAttempts
	}

	if o.RetryBackoff <= 0 {
		o.RetryBackoff = defaultRetryBackoff
	}

	if o.Now == nil {
		o.Now = time.Now
	}

	return o
}

func buildDeadLetterMessage(message Message, dispatchErr error, attempts int, now time.Time) *DeadLetterMessage {
	dlqMessage := &DeadLetterMessage{
		FailedAt:        now.UTC(),
		Attempts:        attempts,
		Reason:          strings.TrimSpace(dispatchErr.Error()),
		OriginalKey:     message.Key,
		OriginalPayload: append(json.RawMessage(nil), message.Value...),
	}

	var commandEnvelope envelope
	if err := json.Unmarshal(message.Value, &commandEnvelope); err != nil {
		return dlqMessage
	}

	dlqMessage.CommandType = commandEnvelope.Type
	dlqMessage.CommandMeta = &commandEnvelope.Meta

	return dlqMessage
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
