package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultHTTPAddr               = ":8093"
	defaultGRPCAddr               = ":9093"
	defaultRTCEventsTopic         = "mint.rtc.events.v1"
	defaultRTCCommandsTopic       = "mint.rtc.commands.v1"
	defaultKafkaConsumerGroup     = "mint.rtc-api.v1"
	defaultOutboxPollInterval     = 2 * time.Second
	defaultOutboxBatchSize        = 100
	defaultTokenTTL               = 5 * time.Minute
	defaultShutdownGracePeriod    = 10 * time.Second
	defaultKafkaBrokers           = "127.0.0.1:9092"
	defaultRedisAddr              = "127.0.0.1:6379"
	defaultRedisDB                = 0
	defaultRedisKeyPrefix         = "mint:rtc"
	defaultScyllaHosts            = "127.0.0.1"
	defaultScyllaPort             = 9042
	defaultScyllaKeyspace         = "mint_rtc"
	defaultScyllaConsistency      = "quorum"
	defaultScyllaAutoCreateSchema = true
)

// Config contains rtc-api runtime configuration with stable defaults.
type Config struct {
	HTTPAddr               string
	GRPCAddr               string
	KafkaBrokers           []string
	RTCEventsTopic         string
	RTCCommandsTopic       string
	KafkaConsumerGroup     string
	LiveKitAPIKey          string
	LiveKitAPISecret       string
	OutboxPollInterval     time.Duration
	OutboxBatchSize        int
	DefaultTokenTTL        time.Duration
	ShutdownGracePeriod    time.Duration
	RedisAddr              string
	RedisPassword          string
	RedisDB                int
	RedisKeyPrefix         string
	ScyllaHosts            []string
	ScyllaPort             int
	ScyllaKeyspace         string
	ScyllaConsistency      string
	ScyllaAutoCreateSchema bool
}

func Default() Config {
	return Config{
		HTTPAddr:               defaultHTTPAddr,
		GRPCAddr:               defaultGRPCAddr,
		KafkaBrokers:           splitCSV(defaultKafkaBrokers),
		RTCEventsTopic:         defaultRTCEventsTopic,
		RTCCommandsTopic:       defaultRTCCommandsTopic,
		KafkaConsumerGroup:     defaultKafkaConsumerGroup,
		OutboxPollInterval:     defaultOutboxPollInterval,
		OutboxBatchSize:        defaultOutboxBatchSize,
		DefaultTokenTTL:        defaultTokenTTL,
		ShutdownGracePeriod:    defaultShutdownGracePeriod,
		RedisAddr:              defaultRedisAddr,
		RedisDB:                defaultRedisDB,
		RedisKeyPrefix:         defaultRedisKeyPrefix,
		ScyllaHosts:            splitCSV(defaultScyllaHosts),
		ScyllaPort:             defaultScyllaPort,
		ScyllaKeyspace:         defaultScyllaKeyspace,
		ScyllaConsistency:      defaultScyllaConsistency,
		ScyllaAutoCreateSchema: defaultScyllaAutoCreateSchema,
	}
}

func LoadFromEnv() (Config, error) {
	cfg := Default()

	cfg.HTTPAddr = stringEnvOrDefault("MINT_RTC_HTTP_ADDR", cfg.HTTPAddr)
	cfg.GRPCAddr = stringEnvOrDefault("MINT_RTC_GRPC_ADDR", cfg.GRPCAddr)
	cfg.KafkaBrokers = csvEnvOrDefault("MINT_KAFKA_BROKERS", cfg.KafkaBrokers)
	cfg.RTCEventsTopic = stringEnvOrDefault("MINT_RTC_EVENTS_TOPIC", cfg.RTCEventsTopic)
	cfg.RTCCommandsTopic = stringEnvOrDefault("MINT_RTC_COMMANDS_TOPIC", cfg.RTCCommandsTopic)
	cfg.KafkaConsumerGroup = stringEnvOrDefault("MINT_RTC_KAFKA_CONSUMER_GROUP", cfg.KafkaConsumerGroup)

	cfg.LiveKitAPIKey = os.Getenv("MINT_LIVEKIT_API_KEY")
	cfg.LiveKitAPISecret = os.Getenv("MINT_LIVEKIT_API_SECRET")

	pollInterval, err := durationEnvOrDefault("MINT_RTC_OUTBOX_POLL_INTERVAL", cfg.OutboxPollInterval)
	if err != nil {
		return Config{}, err
	}

	cfg.OutboxPollInterval = pollInterval

	batchSize, err := intEnvOrDefault("MINT_RTC_OUTBOX_BATCH_SIZE", cfg.OutboxBatchSize)
	if err != nil {
		return Config{}, err
	}

	cfg.OutboxBatchSize = batchSize

	tokenTTL, err := durationEnvOrDefault("MINT_RTC_DEFAULT_TOKEN_TTL", cfg.DefaultTokenTTL)
	if err != nil {
		return Config{}, err
	}

	cfg.DefaultTokenTTL = tokenTTL

	shutdownGracePeriod, err := durationEnvOrDefault("MINT_RTC_SHUTDOWN_GRACE_PERIOD", cfg.ShutdownGracePeriod)
	if err != nil {
		return Config{}, err
	}

	cfg.ShutdownGracePeriod = shutdownGracePeriod

	cfg.RedisAddr = stringEnvOrDefault("MINT_REDIS_ADDR", cfg.RedisAddr)
	cfg.RedisPassword = os.Getenv("MINT_REDIS_PASSWORD")

	redisDB, err := intEnvOrDefault("MINT_REDIS_DB", cfg.RedisDB)
	if err != nil {
		return Config{}, err
	}

	cfg.RedisDB = redisDB
	cfg.RedisKeyPrefix = stringEnvOrDefault("MINT_REDIS_KEY_PREFIX", cfg.RedisKeyPrefix)

	cfg.ScyllaHosts = csvEnvOrDefault("MINT_SCYLLA_HOSTS", cfg.ScyllaHosts)

	scyllaPort, err := intEnvOrDefault("MINT_SCYLLA_PORT", cfg.ScyllaPort)
	if err != nil {
		return Config{}, err
	}

	cfg.ScyllaPort = scyllaPort
	cfg.ScyllaKeyspace = stringEnvOrDefault("MINT_SCYLLA_KEYSPACE", cfg.ScyllaKeyspace)
	cfg.ScyllaConsistency = stringEnvOrDefault("MINT_SCYLLA_CONSISTENCY", cfg.ScyllaConsistency)

	autoCreateSchema, err := boolEnvOrDefault("MINT_SCYLLA_AUTO_CREATE_SCHEMA", cfg.ScyllaAutoCreateSchema)
	if err != nil {
		return Config{}, err
	}

	cfg.ScyllaAutoCreateSchema = autoCreateSchema

	if cfg.OutboxBatchSize <= 0 {
		return Config{}, fmt.Errorf("MINT_RTC_OUTBOX_BATCH_SIZE must be > 0")
	}

	if cfg.OutboxPollInterval <= 0 {
		return Config{}, fmt.Errorf("MINT_RTC_OUTBOX_POLL_INTERVAL must be > 0")
	}

	if cfg.DefaultTokenTTL <= 0 {
		return Config{}, fmt.Errorf("MINT_RTC_DEFAULT_TOKEN_TTL must be > 0")
	}

	if cfg.ShutdownGracePeriod <= 0 {
		return Config{}, fmt.Errorf("MINT_RTC_SHUTDOWN_GRACE_PERIOD must be > 0")
	}

	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("MINT_KAFKA_BROKERS must contain at least one broker")
	}

	if len(cfg.ScyllaHosts) == 0 {
		return Config{}, fmt.Errorf("MINT_SCYLLA_HOSTS must contain at least one host")
	}

	if cfg.ScyllaPort <= 0 {
		return Config{}, fmt.Errorf("MINT_SCYLLA_PORT must be > 0")
	}

	if strings.TrimSpace(cfg.ScyllaKeyspace) == "" {
		return Config{}, fmt.Errorf("MINT_SCYLLA_KEYSPACE must not be empty")
	}

	if strings.TrimSpace(cfg.RedisAddr) == "" {
		return Config{}, fmt.Errorf("MINT_REDIS_ADDR must not be empty")
	}

	if strings.TrimSpace(cfg.RTCEventsTopic) == "" {
		return Config{}, fmt.Errorf("MINT_RTC_EVENTS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.RTCCommandsTopic) == "" {
		return Config{}, fmt.Errorf("MINT_RTC_COMMANDS_TOPIC must not be empty")
	}

	if strings.TrimSpace(cfg.KafkaConsumerGroup) == "" {
		return Config{}, fmt.Errorf("MINT_RTC_KAFKA_CONSUMER_GROUP must not be empty")
	}

	return cfg, nil
}

func stringEnvOrDefault(envName string, fallback string) string {
	value := os.Getenv(envName)
	if value == "" {
		return fallback
	}

	return value
}

func durationEnvOrDefault(envName string, fallback time.Duration) (time.Duration, error) {
	value := os.Getenv(envName)
	if value == "" {
		return fallback, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", envName, err)
	}

	return duration, nil
}

func intEnvOrDefault(envName string, fallback int) (int, error) {
	value := os.Getenv(envName)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", envName, err)
	}

	return parsed, nil
}

func boolEnvOrDefault(envName string, fallback bool) (bool, error) {
	value := os.Getenv(envName)
	if value == "" {
		return fallback, nil
	}

	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", envName, err)
	}

	return parsed, nil
}

func csvEnvOrDefault(envName string, fallback []string) []string {
	value := os.Getenv(envName)
	if value == "" {
		copied := make([]string, len(fallback))
		copy(copied, fallback)
		return copied
	}

	return splitCSV(value)
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
