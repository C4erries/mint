package config

import "time"

const (
	defaultHTTPAddr                      = ":8093"
	defaultGRPCAddr                      = ":9093"
	defaultRTCEventsTopic                = "mint.rtc.events.v1"
	defaultRTCCommandsTopic              = "mint.rtc.commands.v1"
	defaultRTCCommandsDLQTopic           = "mint.rtc.commands.dlq.v1"
	defaultRTCCommandMaxDispatchAttempts = 3
	defaultRTCCommandRetryBackoff        = 200 * time.Millisecond
	defaultKafkaConsumerGroup            = "mint.rtc-api.v1"
	defaultLiveKitURL                    = "http://127.0.0.1:7880"
	defaultOutboxPollInterval            = 2 * time.Second
	defaultOutboxBatchSize               = 100
	defaultTokenTTL                      = 5 * time.Minute
	defaultShutdownGracePeriod           = 10 * time.Second
	defaultKafkaBrokers                  = "127.0.0.1:9092"
	defaultRedisAddr                     = "127.0.0.1:6379"
	defaultRedisDB                       = 0
	defaultRedisKeyPrefix                = "mint:rtc"
	defaultScyllaHosts                   = "127.0.0.1"
	defaultScyllaPort                    = 9042
	defaultScyllaKeyspace                = "mint_rtc"
	defaultScyllaConsistency             = "quorum"
	defaultScyllaAutoCreateSchema        = true
	defaultPermissionGRPCAddr            = "127.0.0.1:9091"
	defaultPermissionTimeout             = 300 * time.Millisecond
	defaultPermissionMaxRetries          = 2
	defaultPermissionRetryBackoff        = 100 * time.Millisecond
)

// Config contains rtc-api runtime configuration with stable defaults.
type Config struct {
	HTTPAddr                      string
	GRPCAddr                      string
	KafkaBrokers                  []string
	RTCEventsTopic                string
	RTCCommandsTopic              string
	RTCCommandsDLQTopic           string
	RTCCommandMaxDispatchAttempts int
	RTCCommandRetryBackoff        time.Duration
	KafkaConsumerGroup            string
	LiveKitURL                    string
	LiveKitAPIKey                 string
	LiveKitAPISecret              string
	OutboxPollInterval            time.Duration
	OutboxBatchSize               int
	DefaultTokenTTL               time.Duration
	ShutdownGracePeriod           time.Duration
	RedisAddr                     string
	RedisPassword                 string
	RedisDB                       int
	RedisKeyPrefix                string
	ScyllaHosts                   []string
	ScyllaPort                    int
	ScyllaKeyspace                string
	ScyllaConsistency             string
	ScyllaAutoCreateSchema        bool
	PermissionGRPCAddr            string
	PermissionTimeout             time.Duration
	PermissionMaxRetries          int
	PermissionRetryBackoff        time.Duration
}

func Default() Config {
	return Config{
		HTTPAddr:                      defaultHTTPAddr,
		GRPCAddr:                      defaultGRPCAddr,
		KafkaBrokers:                  splitCSV(defaultKafkaBrokers),
		RTCEventsTopic:                defaultRTCEventsTopic,
		RTCCommandsTopic:              defaultRTCCommandsTopic,
		RTCCommandsDLQTopic:           defaultRTCCommandsDLQTopic,
		RTCCommandMaxDispatchAttempts: defaultRTCCommandMaxDispatchAttempts,
		RTCCommandRetryBackoff:        defaultRTCCommandRetryBackoff,
		KafkaConsumerGroup:            defaultKafkaConsumerGroup,
		LiveKitURL:                    defaultLiveKitURL,
		OutboxPollInterval:            defaultOutboxPollInterval,
		OutboxBatchSize:               defaultOutboxBatchSize,
		DefaultTokenTTL:               defaultTokenTTL,
		ShutdownGracePeriod:           defaultShutdownGracePeriod,
		RedisAddr:                     defaultRedisAddr,
		RedisDB:                       defaultRedisDB,
		RedisKeyPrefix:                defaultRedisKeyPrefix,
		ScyllaHosts:                   splitCSV(defaultScyllaHosts),
		ScyllaPort:                    defaultScyllaPort,
		ScyllaKeyspace:                defaultScyllaKeyspace,
		ScyllaConsistency:             defaultScyllaConsistency,
		ScyllaAutoCreateSchema:        defaultScyllaAutoCreateSchema,
		PermissionGRPCAddr:            defaultPermissionGRPCAddr,
		PermissionTimeout:             defaultPermissionTimeout,
		PermissionMaxRetries:          defaultPermissionMaxRetries,
		PermissionRetryBackoff:        defaultPermissionRetryBackoff,
	}
}

func LoadFromEnv() (Config, error) {
	cfg := Default()

	cfg.HTTPAddr = stringEnvOrDefault("MINT_RTC_HTTP_ADDR", cfg.HTTPAddr)
	cfg.GRPCAddr = stringEnvOrDefault("MINT_RTC_GRPC_ADDR", cfg.GRPCAddr)
	cfg.KafkaBrokers = csvEnvOrDefault("MINT_KAFKA_BROKERS", cfg.KafkaBrokers)
	cfg.RTCEventsTopic = stringEnvOrDefault("MINT_RTC_EVENTS_TOPIC", cfg.RTCEventsTopic)
	cfg.RTCCommandsTopic = stringEnvOrDefault("MINT_RTC_COMMANDS_TOPIC", cfg.RTCCommandsTopic)
	cfg.RTCCommandsDLQTopic = stringEnvOrDefault("MINT_RTC_COMMANDS_DLQ_TOPIC", cfg.RTCCommandsDLQTopic)
	cfg.KafkaConsumerGroup = stringEnvOrDefault("MINT_RTC_KAFKA_CONSUMER_GROUP", cfg.KafkaConsumerGroup)
	cfg.LiveKitURL = stringEnvOrDefault("MINT_LIVEKIT_URL", cfg.LiveKitURL)
	cfg.LiveKitAPIKey = env("MINT_LIVEKIT_API_KEY")
	cfg.LiveKitAPISecret = env("MINT_LIVEKIT_API_SECRET")

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

	maxDispatchAttempts, err := intEnvOrDefault("MINT_RTC_COMMAND_MAX_DISPATCH_ATTEMPTS", cfg.RTCCommandMaxDispatchAttempts)
	if err != nil {
		return Config{}, err
	}

	cfg.RTCCommandMaxDispatchAttempts = maxDispatchAttempts

	retryBackoff, err := durationEnvOrDefault("MINT_RTC_COMMAND_RETRY_BACKOFF", cfg.RTCCommandRetryBackoff)
	if err != nil {
		return Config{}, err
	}

	cfg.RTCCommandRetryBackoff = retryBackoff

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

	cfg.PermissionGRPCAddr = stringEnvOrDefault("MINT_PERMISSION_GRPC_ADDR", cfg.PermissionGRPCAddr)

	permissionTimeout, err := durationEnvOrDefault("MINT_PERMISSION_TIMEOUT", cfg.PermissionTimeout)
	if err != nil {
		return Config{}, err
	}

	cfg.PermissionTimeout = permissionTimeout

	permissionMaxRetries, err := intEnvOrDefault("MINT_PERMISSION_MAX_RETRIES", cfg.PermissionMaxRetries)
	if err != nil {
		return Config{}, err
	}
	cfg.PermissionMaxRetries = permissionMaxRetries

	permissionRetryBackoff, err := durationEnvOrDefault("MINT_PERMISSION_RETRY_BACKOFF", cfg.PermissionRetryBackoff)
	if err != nil {
		return Config{}, err
	}
	cfg.PermissionRetryBackoff = permissionRetryBackoff

	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}

	return cfg, nil
}
