package jwt

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/c4erries/mint/internal/identity/application"
)

type tokenClaims struct {
	TokenType string `json:"token_type"`
	AccountID string `json:"account_id"`
	SessionID string `json:"session_id"`
	jwt.RegisteredClaims
}

// Manager issues and validates identity JWTs.
type Manager struct {
	accessSecret  []byte
	refreshSecret []byte
	idGenerator   func() string
}

func NewManager(accessSecret string, refreshSecret string, idGenerator func() string) (*Manager, error) {
	if strings.TrimSpace(accessSecret) == "" || strings.TrimSpace(refreshSecret) == "" {
		return nil, fmt.Errorf("jwt secrets are required")
	}

	if idGenerator == nil {
		return nil, fmt.Errorf("jwt id generator is required")
	}

	return &Manager{accessSecret: []byte(accessSecret), refreshSecret: []byte(refreshSecret), idGenerator: idGenerator}, nil
}

func (m *Manager) IssueToken(accountID string, sessionID string, tokenType application.TokenType, ttl time.Duration, now time.Time) (application.IssuedToken, error) {
	if accountID == "" || sessionID == "" || ttl <= 0 || now.IsZero() {
		return application.IssuedToken{}, application.ErrInvalidCommand
	}

	tokenID := m.idGenerator()
	expiresAt := now.UTC().Add(ttl)
	claims := tokenClaims{
		TokenType: string(tokenType),
		AccountID: accountID,
		SessionID: sessionID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   accountID,
			ID:        tokenID,
			IssuedAt:  jwt.NewNumericDate(now.UTC()),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	raw, err := token.SignedString(m.secretForType(tokenType))
	if err != nil {
		return application.IssuedToken{}, fmt.Errorf("sign jwt token: %w", err)
	}

	return application.IssuedToken{
		Raw: raw,
		Claims: application.TokenClaims{
			TokenID:   tokenID,
			AccountID: accountID,
			SessionID: sessionID,
			TokenType: tokenType,
			IssuedAt:  now.UTC(),
			ExpiresAt: expiresAt,
		},
	}, nil
}

func (m *Manager) ParseToken(rawToken string, expectedType application.TokenType) (application.TokenClaims, error) {
	trimmed := strings.TrimSpace(rawToken)
	if trimmed == "" {
		return application.TokenClaims{}, application.ErrTokenInvalid
	}

	claims := &tokenClaims{}
	token, err := jwt.ParseWithClaims(trimmed, claims, func(token *jwt.Token) (interface{}, error) {
		if token.Method.Alg() != jwt.SigningMethodHS256.Alg() {
			return nil, application.ErrTokenInvalid
		}

		return m.secretForType(expectedType), nil
	})
	if err != nil {
		if errorsAsTokenExpired(err) {
			return application.TokenClaims{}, application.ErrTokenExpired
		}

		return application.TokenClaims{}, application.ErrTokenInvalid
	}

	if token == nil || !token.Valid {
		return application.TokenClaims{}, application.ErrTokenInvalid
	}

	if application.TokenType(claims.TokenType) != expectedType {
		return application.TokenClaims{}, application.ErrTokenInvalid
	}

	if claims.ExpiresAt == nil || claims.IssuedAt == nil {
		return application.TokenClaims{}, application.ErrTokenInvalid
	}

	if claims.ID == "" || claims.AccountID == "" || claims.SessionID == "" {
		return application.TokenClaims{}, application.ErrTokenInvalid
	}

	return application.TokenClaims{
		TokenID:   claims.ID,
		AccountID: claims.AccountID,
		SessionID: claims.SessionID,
		TokenType: expectedType,
		IssuedAt:  claims.IssuedAt.Time,
		ExpiresAt: claims.ExpiresAt.Time,
	}, nil
}

func (m *Manager) secretForType(tokenType application.TokenType) []byte {
	if tokenType == application.TokenTypeRefresh {
		return m.refreshSecret
	}

	return m.accessSecret
}

func errorsAsTokenExpired(err error) bool {
	return errors.Is(err, jwt.ErrTokenExpired)
}
