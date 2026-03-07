package consumer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"
)

const (
	defaultMaxDispatchAttempts = 3
	defaultRetryBackoff        = 200 * time.Millisecond
)

// Reader is a minimal message source abstraction with explicit ack.
type Reader[M any] interface {
	Poll(ctx context.Context) (M, error)
	Ack(ctx context.Context, message M) error
}

// Options configures dispatch retry and logging behavior.
type Options struct {
	MaxDispatchAttempts int
	RetryBackoff        time.Duration
	Now                 func() time.Time
	Logger              *slog.Logger
	Name                string
}

// Runner provides reusable poll-dispatch-retry-dlq-ack orchestration.
type Runner[M any, D any] struct {
	reader          Reader[M]
	dispatch        func(ctx context.Context, message M) error
	buildDeadLetter func(message M, dispatchErr error, attempts int, failedAt time.Time) D
	publishDead     func(ctx context.Context, message D) error
	options         Options
}

func NewRunner[M any, D any](
	reader Reader[M],
	dispatch func(ctx context.Context, message M) error,
	buildDeadLetter func(message M, dispatchErr error, attempts int, failedAt time.Time) D,
	publishDead func(ctx context.Context, message D) error,
	options Options,
) *Runner[M, D] {
	return &Runner[M, D]{
		reader:          reader,
		dispatch:        dispatch,
		buildDeadLetter: buildDeadLetter,
		publishDead:     publishDead,
		options:         options.withDefaults(),
	}
}

func (o Options) withDefaults() Options {
	if o.MaxDispatchAttempts <= 0 {
		o.MaxDispatchAttempts = defaultMaxDispatchAttempts
	}

	if o.RetryBackoff <= 0 {
		o.RetryBackoff = defaultRetryBackoff
	}

	if o.Now == nil {
		o.Now = time.Now
	}

	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	o.Name = strings.TrimSpace(o.Name)
	if o.Name == "" {
		o.Name = "message"
	}

	return o
}

func (r *Runner[M, D]) Run(ctx context.Context) error {
	if r == nil {
		return nil
	}

	if r.reader == nil {
		return fmt.Errorf("%s reader is not configured", r.options.Name)
	}

	if r.dispatch == nil {
		return fmt.Errorf("%s dispatch function is not configured", r.options.Name)
	}

	for {
		message, err := r.reader.Poll(ctx)
		if err != nil {
			if isContextTerminated(err) {
				return nil
			}

			r.options.Logger.Error(
				"failed to poll message",
				slog.String("consumer", r.options.Name),
				slog.String("error", err.Error()),
			)

			continue
		}

		shouldAck, err := r.processMessage(ctx, message)
		if err != nil {
			if isContextTerminated(err) {
				return nil
			}

			r.options.Logger.Error(
				"failed to process message",
				slog.String("consumer", r.options.Name),
				slog.String("error", err.Error()),
			)
		}

		if !shouldAck {
			continue
		}

		if err = r.reader.Ack(ctx, message); err != nil {
			if isContextTerminated(err) {
				return nil
			}

			r.options.Logger.Error(
				"failed to ack message",
				slog.String("consumer", r.options.Name),
				slog.String("error", err.Error()),
			)
		}
	}
}

func (r *Runner[M, D]) processMessage(ctx context.Context, message M) (bool, error) {
	var dispatchErr error

	for attempt := 1; attempt <= r.options.MaxDispatchAttempts; attempt++ {
		dispatchErr = r.dispatch(ctx, message)
		if dispatchErr == nil {
			return true, nil
		}

		if isContextTerminated(dispatchErr) {
			return false, dispatchErr
		}

		if attempt == r.options.MaxDispatchAttempts {
			break
		}

		r.options.Logger.Warn(
			"dispatch failed, retrying",
			slog.String("consumer", r.options.Name),
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", r.options.MaxDispatchAttempts),
			slog.String("error", dispatchErr.Error()),
		)

		if err := waitForRetry(ctx, r.options.RetryBackoff); err != nil {
			return false, err
		}
	}

	if r.buildDeadLetter == nil || r.publishDead == nil {
		return false, fmt.Errorf("%s dead letter publisher is not configured: %w", r.options.Name, dispatchErr)
	}

	dlqMessage := r.buildDeadLetter(message, dispatchErr, r.options.MaxDispatchAttempts, r.options.Now().UTC())
	if err := r.publishDead(ctx, dlqMessage); err != nil {
		return false, fmt.Errorf("publish %s dead letter message: %w", r.options.Name, err)
	}

	return true, nil
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

func isContextTerminated(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
