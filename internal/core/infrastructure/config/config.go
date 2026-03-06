package config

import "time"

const (
	defaultHTTPAddr               = ":8091"
	defaultGRPCAddr               = ":9091"
	defaultShutdownGracePeriod    = 10 * time.Second
	defaultKafkaBrokers           = "127.0.0.1:9092"
	defaultRTCCommandsTopic       = "mint.rtc.commands.v1"
	defaultWorkspaceCommandsTopic = "mint.workspace.commands.v1"
	defaultWorkspaceEventsTopic   = "mint.workspace.events.v1"
	defaultWorkspaceConsumerGroup = "mint.core.workspace.v1"
	defaultOutboxPollInterval     = 2 * time.Second
	defaultOutboxBatchSize        = 100
	defaultRedisAddr              = "127.0.0.1:6379"
	defaultRedisDB                = 0
	defaultRedisKeyPrefix         = "mint:core"
	defaultScyllaHosts            = "127.0.0.1"
	defaultScyllaPort             = 9042
	defaultScyllaKeyspace         = "mint_core"
	defaultScyllaConsistency      = "quorum"
	defaultScyllaAutoCreateSchema = true
	defaultJWTAccessSecret        = "dev-access-secret"
	defaultJWTRefreshSecret       = "dev-refresh-secret"
	defaultJWTAccessTTL           = 15 * time.Minute
	defaultJWTRefreshTTL          = 7 * 24 * time.Hour
	defaultRTCGRPCAddr            = "127.0.0.1:9093"
	defaultRTCGRPCTimeout         = 300 * time.Millisecond
	defaultRTCGRPCMaxRetries      = 2
	defaultRTCGRPCRetryBackoff    = 100 * time.Millisecond
)

// Config stores runtime configuration for core service.
type Config struct {
	HTTPAddr            string
	GRPCAddr            string
	ShutdownGracePeriod time.Duration

	KafkaBrokers           []string
	RTCCommandsTopic       string
	WorkspaceCommandsTopic string
	WorkspaceEventsTopic   string
	WorkspaceConsumerGroup string
	OutboxPollInterval     time.Duration
	OutboxBatchSize        int

	RedisAddr      string
	RedisPassword  string
	RedisDB        int
	RedisKeyPrefix string

	ScyllaHosts            []string
	ScyllaPort             int
	ScyllaKeyspace         string
	ScyllaConsistency      string
	ScyllaAutoCreateSchema bool

	JWTAccessSecret  string
	JWTRefreshSecret string
	JWTAccessTTL     time.Duration
	JWTRefreshTTL    time.Duration

	RTCGRPCAddr         string
	RTCGRPCTimeout      time.Duration
	RTCGRPCMaxRetries   int
	RTCGRPCRetryBackoff time.Duration
}

func Default() Config {
	return Config{
		HTTPAddr:               defaultHTTPAddr,
		GRPCAddr:               defaultGRPCAddr,
		ShutdownGracePeriod:    defaultShutdownGracePeriod,
		KafkaBrokers:           splitCSV(defaultKafkaBrokers),
		RTCCommandsTopic:       defaultRTCCommandsTopic,
		WorkspaceCommandsTopic: defaultWorkspaceCommandsTopic,
		WorkspaceEventsTopic:   defaultWorkspaceEventsTopic,
		WorkspaceConsumerGroup: defaultWorkspaceConsumerGroup,
		OutboxPollInterval:     defaultOutboxPollInterval,
		OutboxBatchSize:        defaultOutboxBatchSize,
		RedisAddr:              defaultRedisAddr,
		RedisDB:                defaultRedisDB,
		RedisKeyPrefix:         defaultRedisKeyPrefix,
		ScyllaHosts:            splitCSV(defaultScyllaHosts),
		ScyllaPort:             defaultScyllaPort,
		ScyllaKeyspace:         defaultScyllaKeyspace,
		ScyllaConsistency:      defaultScyllaConsistency,
		ScyllaAutoCreateSchema: defaultScyllaAutoCreateSchema,
		JWTAccessSecret:        defaultJWTAccessSecret,
		JWTRefreshSecret:       defaultJWTRefreshSecret,
		JWTAccessTTL:           defaultJWTAccessTTL,
		JWTRefreshTTL:          defaultJWTRefreshTTL,
		RTCGRPCAddr:            defaultRTCGRPCAddr,
		RTCGRPCTimeout:         defaultRTCGRPCTimeout,
		RTCGRPCMaxRetries:      defaultRTCGRPCMaxRetries,
		RTCGRPCRetryBackoff:    defaultRTCGRPCRetryBackoff,
	}
}

func LoadFromEnv() (Config, error) {
	cfg := Default()

	cfg.HTTPAddr = stringEnvOrDefault("MINT_CORE_HTTP_ADDR", cfg.HTTPAddr)
	cfg.GRPCAddr = stringEnvOrDefault("MINT_CORE_GRPC_ADDR", cfg.GRPCAddr)

	shutdownGracePeriod, err := durationEnvOrDefault("MINT_CORE_SHUTDOWN_GRACE_PERIOD", cfg.ShutdownGracePeriod)
	if err != nil {
		return Config{}, err
	}
	cfg.ShutdownGracePeriod = shutdownGracePeriod

	cfg.KafkaBrokers = csvEnvOrDefault("MINT_KAFKA_BROKERS", cfg.KafkaBrokers)
	cfg.RTCCommandsTopic = stringEnvOrDefault("MINT_RTC_COMMANDS_TOPIC", cfg.RTCCommandsTopic)
	cfg.WorkspaceCommandsTopic = stringEnvOrDefault("MINT_CORE_WORKSPACE_COMMANDS_TOPIC", cfg.WorkspaceCommandsTopic)
	cfg.WorkspaceEventsTopic = stringEnvOrDefault("MINT_CORE_WORKSPACE_EVENTS_TOPIC", cfg.WorkspaceEventsTopic)
	cfg.WorkspaceConsumerGroup = stringEnvOrDefault("MINT_CORE_WORKSPACE_CONSUMER_GROUP", cfg.WorkspaceConsumerGroup)

	outboxPollInterval, err := durationEnvOrDefault("MINT_CORE_OUTBOX_POLL_INTERVAL", cfg.OutboxPollInterval)
	if err != nil {
		return Config{}, err
	}
	cfg.OutboxPollInterval = outboxPollInterval

	outboxBatchSize, err := intEnvOrDefault("MINT_CORE_OUTBOX_BATCH_SIZE", cfg.OutboxBatchSize)
	if err != nil {
		return Config{}, err
	}
	cfg.OutboxBatchSize = outboxBatchSize

	cfg.RedisAddr = stringEnvOrDefault("MINT_REDIS_ADDR", cfg.RedisAddr)
	cfg.RedisPassword = env("MINT_REDIS_PASSWORD")

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

	cfg.JWTAccessSecret = stringEnvOrDefault("MINT_CORE_JWT_ACCESS_SECRET", cfg.JWTAccessSecret)
	cfg.JWTRefreshSecret = stringEnvOrDefault("MINT_CORE_JWT_REFRESH_SECRET", cfg.JWTRefreshSecret)

	accessTTL, err := durationEnvOrDefault("MINT_CORE_JWT_ACCESS_TTL", cfg.JWTAccessTTL)
	if err != nil {
		return Config{}, err
	}
	cfg.JWTAccessTTL = accessTTL

	refreshTTL, err := durationEnvOrDefault("MINT_CORE_JWT_REFRESH_TTL", cfg.JWTRefreshTTL)
	if err != nil {
		return Config{}, err
	}
	cfg.JWTRefreshTTL = refreshTTL

	cfg.RTCGRPCAddr = stringEnvOrDefault("MINT_RTC_GRPC_ADDR", cfg.RTCGRPCAddr)

	rtcTimeout, err := durationEnvOrDefault("MINT_CORE_RTC_GRPC_TIMEOUT", cfg.RTCGRPCTimeout)
	if err != nil {
		return Config{}, err
	}
	cfg.RTCGRPCTimeout = rtcTimeout

	rtcMaxRetries, err := intEnvOrDefault("MINT_CORE_RTC_GRPC_MAX_RETRIES", cfg.RTCGRPCMaxRetries)
	if err != nil {
		return Config{}, err
	}
	cfg.RTCGRPCMaxRetries = rtcMaxRetries

	rtcRetryBackoff, err := durationEnvOrDefault("MINT_CORE_RTC_GRPC_RETRY_BACKOFF", cfg.RTCGRPCRetryBackoff)
	if err != nil {
		return Config{}, err
	}
	cfg.RTCGRPCRetryBackoff = rtcRetryBackoff

	if err = validateConfig(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
