//go:build smoke

package smoke_test

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

type smokeConfig struct {
	grpcAddr       string
	kafkaBrokers   []string
	commandsTopic  string
	dlqTopic       string
	scyllaHost     string
	scyllaPort     int
	scyllaKeyspace string
}

type dlqCommandMeta struct {
	CommandID string `json:"command_id"`
}

type dlqMessage struct {
	Attempts    int             `json:"attempts"`
	Reason      string          `json:"reason"`
	CommandType string          `json:"command_type"`
	CommandMeta *dlqCommandMeta `json:"command_meta"`
}

func TestRTCSmoke_CommandFlowAndDLQ(t *testing.T) {
	cfg := loadSmokeConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	now := time.Now().UTC()
	workspaceID := "smoke-ws"
	channelID := "smoke-ch"
	roomID := "smoke-room-" + uuid.NewString()

	session, err := newScyllaSession(cfg)
	require.NoError(t, err)
	defer session.Close()

	err = seedRoomState(ctx, session, workspaceID, channelID, roomID, now)
	require.NoError(t, err)

	grpcConn, err := grpc.DialContext(ctx, cfg.grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer grpcConn.Close()

	queryClient := rtcv1.NewRTCQueryServiceClient(grpcConn)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.kafkaBrokers...),
		Topic:        cfg.commandsTopic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchTimeout: 100 * time.Millisecond,
	}
	defer writer.Close()

	terminateCommandID := "smoke-term-" + uuid.NewString()
	terminatePayload, err := marshalCommandEnvelope(
		domain.CommandTerminateVoiceState,
		application.CommandMeta{
			CommandID:     terminateCommandID,
			CorrelationID: terminateCommandID,
			CausationID:   terminateCommandID,
			MessageID:     terminateCommandID,
			OccurredAt:    now,
			WorkspaceID:   workspaceID,
			ChannelID:     channelID,
			RoomID:        roomID,
			ActorID:       "smoke-runner",
			SchemaVersion: 1,
		},
		map[string]any{},
	)
	require.NoError(t, err)

	err = writer.WriteMessages(ctx, kafka.Message{Key: []byte(roomID), Value: terminatePayload})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		response, queryErr := queryClient.GetVoiceRoomState(ctx, &rtcv1.GetVoiceRoomStateRequest{
			WorkspaceId: workspaceID,
			ChannelId:   channelID,
		})
		if queryErr != nil || response.GetState() == nil {
			return false
		}

		return !response.GetState().GetActive()
	}, 35*time.Second, 500*time.Millisecond)

	dlqReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.kafkaBrokers,
		Topic:          cfg.dlqTopic,
		GroupID:        "rtc-smoke-dlq-" + uuid.NewString(),
		StartOffset:    kafka.LastOffset,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 500 * time.Millisecond,
	})
	defer dlqReader.Close()

	badCommandID := "smoke-bad-" + uuid.NewString()
	invalidPayload, err := marshalCommandEnvelope(
		"unknown-command",
		application.CommandMeta{
			CommandID:     badCommandID,
			CorrelationID: badCommandID,
			CausationID:   badCommandID,
			MessageID:     badCommandID,
			OccurredAt:    now,
			WorkspaceID:   workspaceID,
			ChannelID:     channelID,
			ActorID:       "smoke-runner",
			SchemaVersion: 1,
		},
		map[string]any{"foo": "bar"},
	)
	require.NoError(t, err)

	err = writer.WriteMessages(ctx, kafka.Message{Key: []byte("poison-key"), Value: invalidPayload})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
		defer readCancel()

		message, readErr := dlqReader.ReadMessage(readCtx)
		if readErr != nil {
			return false
		}

		var decoded dlqMessage
		if unmarshalErr := json.Unmarshal(message.Value, &decoded); unmarshalErr != nil {
			return false
		}

		if decoded.CommandMeta == nil {
			return false
		}

		return decoded.CommandMeta.CommandID == badCommandID &&
			decoded.CommandType == "unknown-command" &&
			strings.Contains(decoded.Reason, "unsupported command type") &&
			decoded.Attempts > 0
	}, 40*time.Second, 500*time.Millisecond)
}

func loadSmokeConfig() smokeConfig {
	return smokeConfig{
		grpcAddr:       envOrDefault("MINT_SMOKE_RTC_GRPC_ADDR", "127.0.0.1:9093"),
		kafkaBrokers:   splitCSV(envOrDefault("MINT_SMOKE_KAFKA_BROKERS", "127.0.0.1:19092")),
		commandsTopic:  envOrDefault("MINT_SMOKE_RTC_COMMANDS_TOPIC", "mint.rtc.commands.v1"),
		dlqTopic:       envOrDefault("MINT_SMOKE_RTC_COMMANDS_DLQ_TOPIC", "mint.rtc.commands.dlq.v1"),
		scyllaHost:     envOrDefault("MINT_SMOKE_SCYLLA_HOST", "127.0.0.1"),
		scyllaPort:     intEnvOrDefault("MINT_SMOKE_SCYLLA_PORT", 9042),
		scyllaKeyspace: envOrDefault("MINT_SMOKE_SCYLLA_KEYSPACE", "mint_rtc"),
	}
}

func newScyllaSession(cfg smokeConfig) (*gocql.Session, error) {
	cluster := gocql.NewCluster(cfg.scyllaHost)
	cluster.Port = cfg.scyllaPort
	cluster.Keyspace = cfg.scyllaKeyspace
	cluster.Consistency = gocql.Quorum
	cluster.DisableInitialHostLookup = true

	return cluster.CreateSession()
}

func seedRoomState(ctx context.Context, session *gocql.Session, workspaceID string, channelID string, roomID string, now time.Time) error {
	if err := session.Query(
		"INSERT INTO rtc_voice_rooms (room_id, workspace_id, channel_id, active, created_at, updated_at, participants_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
		roomID,
		workspaceID,
		channelID,
		true,
		now,
		now,
		"[]",
	).WithContext(ctx).Exec(); err != nil {
		return err
	}

	return session.Query(
		"INSERT INTO rtc_voice_channel_bindings (workspace_id, channel_id, room_id, bound_at, updated_at) VALUES (?, ?, ?, ?, ?)",
		workspaceID,
		channelID,
		roomID,
		now,
		now,
	).WithContext(ctx).Exec()
}

func marshalCommandEnvelope(commandType string, meta application.CommandMeta, payload map[string]any) ([]byte, error) {
	return json.Marshal(map[string]any{
		"type":    commandType,
		"meta":    meta,
		"payload": payload,
	})
}

func envOrDefault(name string, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	return value
}

func intEnvOrDefault(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}

	return parsed
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))

	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}

		result = append(result, trimmed)
	}

	return result
}
