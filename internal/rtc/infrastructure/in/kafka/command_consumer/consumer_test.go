package commandconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
	inmemoryrepo "github.com/c4erries/mint/internal/rtc/infrastructure/out/repository/inmemory"
)

type scriptedReader struct {
	messages []Message
	acked    []Message
	ackErr   error
}

func (r *scriptedReader) Poll(_ context.Context) (Message, error) {
	if len(r.messages) == 0 {
		return Message{}, context.Canceled
	}

	message := r.messages[0]
	r.messages = r.messages[1:]

	return message, nil
}

func (r *scriptedReader) Ack(_ context.Context, message Message) error {
	r.acked = append(r.acked, message)
	return r.ackErr
}

func (r *scriptedReader) Close() error {
	return nil
}

type fakeDeadLetterPublisher struct {
	messages []DeadLetterMessage
	err      error
}

func (p *fakeDeadLetterPublisher) Publish(_ context.Context, message *DeadLetterMessage) error {
	if message != nil {
		p.messages = append(p.messages, *message)
	}

	return p.err
}

type allowAllPermissionChecker struct{}

func (allowAllPermissionChecker) CanJoinVoiceChannel(_ context.Context, _, _, _ string) (bool, error) {
	return true, nil
}

type noopLiveKitClient struct{}

func (noopLiveKitClient) IssueToken(_ context.Context, _ application.LiveKitTokenRequest) (application.LiveKitIssuedToken, error) {
	now := time.Now().UTC()

	return application.LiveKitIssuedToken{
		TokenID:   "token-id",
		Token:     "token",
		IssuedAt:  now,
		ExpiresAt: now.Add(5 * time.Minute),
	}, nil
}

type countingWriteRepository struct {
	inner         application.VoiceRoomWriteRepository
	failRemaining int
	calls         int
}

func (r *countingWriteRepository) WithTx(ctx context.Context, fn func(tx application.VoiceRoomWriteTx) error) error {
	r.calls++
	if r.failRemaining > 0 {
		r.failRemaining--
		return errors.New("transient write error")
	}

	return r.inner.WithTx(ctx, fn)
}

func TestConsumerRunAckAfterSuccessfulDispatch(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	commandService := newTestCommandService(t, inmemoryrepo.NewRoomStore(), now)
	dlqPublisher := &fakeDeadLetterPublisher{}

	message := Message{Value: mustMarshalEnvelope(t, domain.CommandJoinVoiceChannel, defaultMeta(now), map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, commandService, dlqPublisher, nil, ConsumerOptions{})

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
	require.Empty(t, dlqPublisher.messages)
}

func TestConsumerRunAckAfterDLQPublish(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	dlqPublisher := &fakeDeadLetterPublisher{}
	message := Message{Value: mustMarshalEnvelope(t, "unknown-command", defaultMeta(now), map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, nil, dlqPublisher, nil, ConsumerOptions{
		MaxDispatchAttempts: 2,
		RetryBackoff:        time.Millisecond,
		Now:                 func() time.Time { return now },
	})

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
	require.Len(t, dlqPublisher.messages, 1)
	require.Equal(t, 2, dlqPublisher.messages[0].Attempts)
	require.True(t, strings.Contains(dlqPublisher.messages[0].Reason, "unsupported command type"))
}

func TestConsumerRunDoesNotAckWhenDLQPublishFails(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	dlqPublisher := &fakeDeadLetterPublisher{err: errors.New("dlq unavailable")}
	message := Message{Value: mustMarshalEnvelope(t, "unknown-command", defaultMeta(now), map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, nil, dlqPublisher, nil, ConsumerOptions{MaxDispatchAttempts: 2, RetryBackoff: time.Millisecond})

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, reader.acked)
	require.Len(t, dlqPublisher.messages, 1)
}

func TestConsumerRun_UsesConfiguredAttempts(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	baseStore := inmemoryrepo.NewRoomStore()
	err := baseStore.WithTx(context.Background(), func(tx application.VoiceRoomWriteTx) error {
		room, createErr := domain.NewVoiceRoom("room-1", "ws-1", "ch-1", now)
		if createErr != nil {
			return createErr
		}

		return tx.SaveRoom(context.Background(), room)
	})
	require.NoError(t, err)

	writeRepo := &countingWriteRepository{inner: baseStore, failRemaining: 2}
	commandService := newTestCommandService(t, writeRepo, now)
	dlqPublisher := &fakeDeadLetterPublisher{}

	meta := defaultMeta(now)
	meta.RoomID = "room-1"
	message := Message{Value: mustMarshalEnvelope(t, domain.CommandTerminateVoiceState, meta, map[string]any{})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, commandService, dlqPublisher, nil, ConsumerOptions{
		MaxDispatchAttempts: 3,
		RetryBackoff:        time.Millisecond,
	})

	err = consumer.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
	require.Empty(t, dlqPublisher.messages)
	require.Equal(t, 3, writeRepo.calls)
}

func newTestCommandService(t *testing.T, rooms application.VoiceRoomWriteRepository, now time.Time) *application.CommandService {
	t.Helper()

	service, err := application.NewCommandService(
		rooms,
		inmemoryrepo.NewGrantStore(func() time.Time { return now }),
		allowAllPermissionChecker{},
		noopLiveKitClient{},
		application.CommandServiceOptions{
			DefaultTokenTTL: 5 * time.Minute,
			Now:             func() time.Time { return now },
			IDGenerator:     func() string { return "generated-id" },
		},
	)
	require.NoError(t, err)

	return service
}

func defaultMeta(occurredAt time.Time) application.CommandMeta {
	return application.CommandMeta{
		CommandID:     "cmd-1",
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		MessageID:     "msg-1",
		OccurredAt:    occurredAt,
		WorkspaceID:   "ws-1",
		ChannelID:     "ch-1",
		RoomID:        "",
		ActorID:       "user-1",
		SchemaVersion: 1,
	}
}

func mustMarshalEnvelope(t *testing.T, commandType string, meta application.CommandMeta, payload map[string]any) []byte {
	t.Helper()

	envelope := map[string]any{
		"type":    commandType,
		"meta":    meta,
		"payload": payload,
	}

	encoded, err := json.Marshal(envelope)
	require.NoError(t, err)

	return encoded
}
