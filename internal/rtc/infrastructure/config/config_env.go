package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func env(name string) string {
	return os.Getenv(name)
}

func stringEnvOrDefault(envName, fallback string) string {
	value := env(envName)
	if value == "" {
		return fallback
	}

	return value
}

func durationEnvOrDefault(envName string, fallback time.Duration) (time.Duration, error) {
	value := env(envName)
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
	value := env(envName)
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
	value := env(envName)
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
	value := env(envName)
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
