package config

import (
	"fmt"
	"strings"
)

func validateConfig(cfg Config) error {
	if strings.TrimSpace(cfg.HTTPAddr) == "" {
		return fmt.Errorf("MINT_CORE_HTTP_ADDR must not be empty")
	}

	if strings.TrimSpace(cfg.GRPCAddr) == "" {
		return fmt.Errorf("MINT_CORE_GRPC_ADDR must not be empty")
	}

	if cfg.ShutdownGracePeriod <= 0 {
		return fmt.Errorf("MINT_CORE_SHUTDOWN_GRACE_PERIOD must be > 0")
	}

	if len(cfg.KafkaBrokers) == 0 {
		return fmt.Errorf("MINT_KAFKA_BROKERS must contain at least one broker")
	}

	if strings.TrimSpace(cfg.RTCCommandsTopic) == "" {
		return fmt.Errorf("MINT_RTC_COMMANDS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.WorkspaceCommandsTopic) == "" {
		return fmt.Errorf("MINT_CORE_WORKSPACE_COMMANDS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.WorkspaceCommandsDLQ) == "" {
		return fmt.Errorf("MINT_CORE_WORKSPACE_COMMANDS_DLQ_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.WorkspaceEventsTopic) == "" {
		return fmt.Errorf("MINT_CORE_WORKSPACE_EVENTS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.WorkspaceConsumerGroup) == "" {
		return fmt.Errorf("MINT_CORE_WORKSPACE_CONSUMER_GROUP must not be empty")
	}

	if cfg.WorkspaceCommandMaxAttempts <= 0 {
		return fmt.Errorf("MINT_CORE_WORKSPACE_COMMAND_MAX_DISPATCH_ATTEMPTS must be > 0")
	}

	if cfg.WorkspaceCommandRetryBackoff <= 0 {
		return fmt.Errorf("MINT_CORE_WORKSPACE_COMMAND_RETRY_BACKOFF must be > 0")
	}

	if cfg.OutboxPollInterval <= 0 {
		return fmt.Errorf("MINT_CORE_OUTBOX_POLL_INTERVAL must be > 0")
	}

	if cfg.OutboxBatchSize <= 0 {
		return fmt.Errorf("MINT_CORE_OUTBOX_BATCH_SIZE must be > 0")
	}

	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return fmt.Errorf("MINT_REDIS_ADDR must not be empty")
	}

	if cfg.RedisDB < 0 {
		return fmt.Errorf("MINT_REDIS_DB must be >= 0")
	}

	if strings.TrimSpace(cfg.RedisKeyPrefix) == "" {
		return fmt.Errorf("MINT_REDIS_KEY_PREFIX must not be empty")
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

	if strings.TrimSpace(cfg.JWTAccessSecret) == "" {
		return fmt.Errorf("MINT_CORE_JWT_ACCESS_SECRET must not be empty")
	}

	if strings.TrimSpace(cfg.JWTRefreshSecret) == "" {
		return fmt.Errorf("MINT_CORE_JWT_REFRESH_SECRET must not be empty")
	}

	if cfg.JWTAccessTTL <= 0 {
		return fmt.Errorf("MINT_CORE_JWT_ACCESS_TTL must be > 0")
	}

	if cfg.JWTRefreshTTL <= 0 {
		return fmt.Errorf("MINT_CORE_JWT_REFRESH_TTL must be > 0")
	}

	if strings.TrimSpace(cfg.RTCGRPCAddr) == "" {
		return fmt.Errorf("MINT_RTC_GRPC_ADDR must not be empty")
	}

	if cfg.RTCGRPCTimeout <= 0 {
		return fmt.Errorf("MINT_CORE_RTC_GRPC_TIMEOUT must be > 0")
	}

	if cfg.RTCGRPCMaxRetries < 0 {
		return fmt.Errorf("MINT_CORE_RTC_GRPC_MAX_RETRIES must be >= 0")
	}

	if cfg.RTCGRPCRetryBackoff <= 0 {
		return fmt.Errorf("MINT_CORE_RTC_GRPC_RETRY_BACKOFF must be > 0")
	}

	return nil
}
