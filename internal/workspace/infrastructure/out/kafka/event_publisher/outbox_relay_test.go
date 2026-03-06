package eventpublisher

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/workspace/application"
)

type fakeOutboxStore struct {
	list          []application.OutboxMessage
	listErr       error
	markErr       error
	listLimit     int
	markedEventID []string
}

func (s *fakeOutboxStore) ListUnpublished(_ context.Context, limit int) ([]application.OutboxMessage, error) {
	s.listLimit = limit
	if s.listErr != nil {
		return nil, s.listErr
	}

	return append([]application.OutboxMessage(nil), s.list...), nil
}

func (s *fakeOutboxStore) MarkPublished(_ context.Context, eventID string) error {
	if s.markErr != nil {
		return s.markErr
	}

	s.markedEventID = append(s.markedEventID, eventID)

	return nil
}

type fakeEventPublisher struct {
	published []application.OutboxMessage
	err       error
}

func (p *fakeEventPublisher) Publish(_ context.Context, message application.OutboxMessage) error {
	if p.err != nil {
		return p.err
	}

	p.published = append(p.published, message)

	return nil
}

func TestNewOutboxRelay_Validation(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := NewOutboxRelay(nil, &fakeEventPublisher{}, logger, 1)
	require.Error(t, err)
	require.ErrorContains(t, err, "outbox store is required")

	_, err = NewOutboxRelay(&fakeOutboxStore{}, nil, logger, 1)
	require.Error(t, err)
	require.ErrorContains(t, err, "event publisher is required")

	_, err = NewOutboxRelay(&fakeOutboxStore{}, &fakeEventPublisher{}, logger, 0)
	require.Error(t, err)
	require.ErrorContains(t, err, "batch size must be > 0")
}

func TestOutboxRelay_FlushOnce(t *testing.T) {
	t.Parallel()

	store := &fakeOutboxStore{
		list: []application.OutboxMessage{
			{EventID: "e1", WorkspaceID: "ws1", OccurredAt: time.Now().UTC()},
			{EventID: "e2", WorkspaceID: "ws2", OccurredAt: time.Now().UTC()},
		},
	}
	publisher := &fakeEventPublisher{}

	relay, err := NewOutboxRelay(store, publisher, nil, 50)
	require.NoError(t, err)

	err = relay.FlushOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 50, store.listLimit)
	require.Len(t, publisher.published, 2)
	require.Equal(t, []string{"e1", "e2"}, store.markedEventID)
}

func TestOutboxRelay_FlushOnceStopsOnPublishError(t *testing.T) {
	t.Parallel()

	store := &fakeOutboxStore{
		list: []application.OutboxMessage{
			{EventID: "e1", WorkspaceID: "ws1", OccurredAt: time.Now().UTC()},
		},
	}
	publisher := &fakeEventPublisher{err: errors.New("publish failed")}

	relay, err := NewOutboxRelay(store, publisher, nil, 10)
	require.NoError(t, err)

	err = relay.FlushOnce(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "publish workspace outbox event e1")
	require.Empty(t, store.markedEventID)
}

func TestOutboxRelay_FlushOnceReturnsMarkError(t *testing.T) {
	t.Parallel()

	store := &fakeOutboxStore{
		list:    []application.OutboxMessage{{EventID: "e1", WorkspaceID: "ws1", OccurredAt: time.Now().UTC()}},
		markErr: errors.New("mark failed"),
	}
	publisher := &fakeEventPublisher{}

	relay, err := NewOutboxRelay(store, publisher, nil, 10)
	require.NoError(t, err)

	err = relay.FlushOnce(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "mark workspace outbox event e1 as published")
}
