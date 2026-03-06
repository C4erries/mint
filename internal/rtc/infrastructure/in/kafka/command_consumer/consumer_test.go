package commandconsumer

import (
	"context"
	"encoding/json"
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

type allowAllPermissionChecker struct{}

func (allowAllPermissionChecker) CanJoinVoiceChannel(_ context.Context, _ string, _ string, _ string) (bool, error) {
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

func TestConsumerRunAckAfterSuccessfulDispatch(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	commandService := newTestCommandService(t, now)

	message := Message{Value: mustMarshalEnvelope(t, domain.CommandJoinVoiceChannel, now, map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, commandService, nil)

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Len(t, reader.acked, 1)
	require.Equal(t, message.Value, reader.acked[0].Value)
}

func TestConsumerRunDoesNotAckWhenDispatchFails(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	message := Message{Value: mustMarshalEnvelope(t, "unknown-command", now, map[string]any{"user_id": "user-1"})}
	reader := &scriptedReader{messages: []Message{message}}

	consumer := New(reader, nil, nil)

	err := consumer.Run(context.Background())
	require.NoError(t, err)
	require.Empty(t, reader.acked)
}

func newTestCommandService(t *testing.T, now time.Time) *application.CommandService {
	t.Helper()

	service, err := application.NewCommandService(
		inmemoryrepo.NewRoomStore(),
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

func mustMarshalEnvelope(t *testing.T, commandType string, occurredAt time.Time, payload map[string]any) []byte {
	t.Helper()

	meta := application.CommandMeta{
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

	envelope := map[string]any{
		"type":    commandType,
		"meta":    meta,
		"payload": payload,
	}

	encoded, err := json.Marshal(envelope)
	require.NoError(t, err)

	return encoded
}
