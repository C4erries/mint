package domain

import (
	"strings"
	"time"
)

// Account is an authentication root aggregate.
type Account struct {
	ID           string
	Email        string
	PasswordHash string
	CreatedAt    time.Time
}

func NewAccount(id string, email string, passwordHash string, now time.Time) (Account, error) {
	if strings.TrimSpace(id) == "" {
		return Account{}, ErrInvalidIdentifier
	}

	normalizedEmail := strings.ToLower(strings.TrimSpace(email))
	if !isEmailLike(normalizedEmail) {
		return Account{}, ErrInvalidEmail
	}

	if strings.TrimSpace(passwordHash) == "" {
		return Account{}, ErrInvalidPassword
	}

	if now.IsZero() {
		return Account{}, ErrInvalidTimestamp
	}

	return Account{
		ID:           id,
		Email:        normalizedEmail,
		PasswordHash: passwordHash,
		CreatedAt:    now,
	}, nil
}

func isEmailLike(value string) bool {
	if value == "" {
		return false
	}

	atIndex := strings.Index(value, "@")
	if atIndex <= 0 || atIndex >= len(value)-1 {
		return false
	}

	dotIndex := strings.LastIndex(value, ".")
	if dotIndex <= atIndex+1 || dotIndex >= len(value)-1 {
		return false
	}

	return true
}
