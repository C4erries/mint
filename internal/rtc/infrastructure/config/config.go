package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultHTTPAddr            = ":8093"
	defaultRTCEventsTopic      = "mint.rtc.events.v1"
	defaultOutboxPollInterval  = 2 * time.Second
	defaultOutboxBatchSize     = 100
	defaultTokenTTL            = 5 * time.Minute
	defaultShutdownGracePeriod = 10 * time.Second
)

// Config contains rtc-api runtime configuration with stable defaults.
type Config struct {
	HTTPAddr             string
	RTCEventsTopic       string
	LiveKitWebhookSecret string
	LiveKitAPIKey        string
	LiveKitAPISecret     string
	OutboxPollInterval   time.Duration
	OutboxBatchSize      int
	DefaultTokenTTL      time.Duration
	ShutdownGracePeriod  time.Duration
}

func Default() Config {
	return Config{
		HTTPAddr:            defaultHTTPAddr,
		RTCEventsTopic:      defaultRTCEventsTopic,
		OutboxPollInterval:  defaultOutboxPollInterval,
		OutboxBatchSize:     defaultOutboxBatchSize,
		DefaultTokenTTL:     defaultTokenTTL,
		ShutdownGracePeriod: defaultShutdownGracePeriod,
	}
}

func LoadFromEnv() (Config, error) {
	cfg := Default()

	cfg.HTTPAddr = stringEnvOrDefault("MINT_RTC_HTTP_ADDR", cfg.HTTPAddr)
	cfg.RTCEventsTopic = stringEnvOrDefault("MINT_RTC_EVENTS_TOPIC", cfg.RTCEventsTopic)
	cfg.LiveKitWebhookSecret = os.Getenv("MINT_LIVEKIT_WEBHOOK_SECRET")
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
