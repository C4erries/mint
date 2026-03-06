package config

import (
	"fmt"
	"strings"
)

func validateConfig(cfg Config) error {
	if cfg.OutboxBatchSize <= 0 {
		return fmt.Errorf("MINT_RTC_OUTBOX_BATCH_SIZE must be > 0")
	}

	if cfg.OutboxPollInterval <= 0 {
		return fmt.Errorf("MINT_RTC_OUTBOX_POLL_INTERVAL must be > 0")
	}

	if cfg.DefaultTokenTTL <= 0 {
		return fmt.Errorf("MINT_RTC_DEFAULT_TOKEN_TTL must be > 0")
	}

	if cfg.ShutdownGracePeriod <= 0 {
		return fmt.Errorf("MINT_RTC_SHUTDOWN_GRACE_PERIOD must be > 0")
	}

	if cfg.RTCCommandMaxDispatchAttempts <= 0 {
		return fmt.Errorf("MINT_RTC_COMMAND_MAX_DISPATCH_ATTEMPTS must be > 0")
	}

	if cfg.RTCCommandRetryBackoff <= 0 {
		return fmt.Errorf("MINT_RTC_COMMAND_RETRY_BACKOFF must be > 0")
	}

	if len(cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("MINT_KAFKA_BROKERS must contain at least one broker")
	}

	if len(cfg.ScyllaHosts) == 0 {
		return fmt.Errorf("MINT_SCYLLA_HOSTS must contain at least one host")
	}

	if cfg.ScyllaPort <= 0 {
		return fmt.Errorf("MINT_SCYLLA_PORT must be > 0")
	}

	if strings.TrimSpace(cfg.ScyllaKeyspace) == "" {
		return fmt.Errorf("MINT_SCYLLA_KEYSPACE must not be empty")
	}

	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return fmt.Errorf("MINT_REDIS_ADDR must not be empty")
	}

	if strings.TrimSpace(cfg.RTCEventsTopic) == "" {
		return fmt.Errorf("MINT_RTC_EVENTS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.RTCCommandsTopic) == "" {
		return fmt.Errorf("MINT_RTC_COMMANDS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.RTCCommandsDLQTopic) == "" {
		return fmt.Errorf("MINT_RTC_COMMANDS_DLQ_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.KafkaConsumerGroup) == "" {
		return fmt.Errorf("MINT_RTC_KAFKA_CONSUMER_GROUP must not be empty")
	}

	if strings.TrimSpace(cfg.LiveKitURL) == "" {
		return fmt.Errorf("MINT_LIVEKIT_URL must not be empty")
	}

	if strings.TrimSpace(cfg.PermissionGRPCAddr) == "" {
		return fmt.Errorf("MINT_PERMISSION_GRPC_ADDR must not be empty")
	}

	if cfg.PermissionTimeout <= 0 {
		return fmt.Errorf("MINT_PERMISSION_TIMEOUT must be > 0")
	}

	if cfg.PermissionMaxRetries < 0 {
		return fmt.Errorf("MINT_PERMISSION_MAX_RETRIES must be >= 0")
	}

	if cfg.PermissionRetryBackoff <= 0 {
		return fmt.Errorf("MINT_PERMISSION_RETRY_BACKOFF must be > 0")
	}

	return nil
}
