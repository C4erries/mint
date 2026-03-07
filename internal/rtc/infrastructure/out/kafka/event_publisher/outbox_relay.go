package eventpublisher

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
)

// OutboxRelay publishes pending outbox records and marks them as published.
type OutboxRelay struct {
	store     application.OutboxStore
	publisher application.EventPublisher
	logger    *slog.Logger
	batchSize int
}

func NewOutboxRelay(
	store application.OutboxStore,
	publisher application.EventPublisher,
	logger *slog.Logger,
	batchSize int,
) (*OutboxRelay, error) {
	if store == nil {
		return nil, fmt.Errorf("outbox store is required")
	}

	if publisher == nil {
		return nil, fmt.Errorf("event publisher is required")
	}

	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	if batchSize <= 0 {
		return nil, fmt.Errorf("batch size must be > 0")
	}

	return &OutboxRelay{
		store:     store,
		publisher: publisher,
		logger:    logger,
		batchSize: batchSize,
	}, nil
}

func (r *OutboxRelay) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("outbox relay interval must be > 0")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.FlushOnce(ctx); err != nil {
				r.logger.Error("outbox relay flush failed", slog.String("error", err.Error()))
			}
		}
	}
}

func (r *OutboxRelay) FlushOnce(ctx context.Context) error {
	messages, err := r.store.ListUnpublished(ctx, r.batchSize)
	if err != nil {
		return fmt.Errorf("list unpublished outbox messages: %w", err)
	}

	for _, message := range messages {
		if err = r.publisher.Publish(ctx, message); err != nil {
			return fmt.Errorf("publish outbox event %s: %w", message.EventID, err)
		}

		if err = r.store.MarkPublished(ctx, message.EventID); err != nil {
			return fmt.Errorf("mark outbox event %s as published: %w", message.EventID, err)
		}
	}

	if len(messages) > 0 {
		r.logger.Info("outbox relay published events", slog.Int("published_events", len(messages)))
	}

	return nil
}
