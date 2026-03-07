package application

import (
	"context"
	"time"

	"github.com/c4erries/mint/internal/identity/domain"
)

// AccountRepository persists account write/read model.
type AccountRepository interface {
	CreateAccount(ctx context.Context, account domain.Account) error
	GetAccountByEmail(ctx context.Context, email string) (domain.Account, error)
	GetAccountByID(ctx context.Context, accountID string) (domain.Account, error)
}

// SessionRepository persists refresh sessions.
type SessionRepository interface {
	CreateSession(ctx context.Context, session domain.Session) error
	GetSessionByRefreshJTI(ctx context.Context, refreshJTI string) (domain.Session, error)
	UpdateSessionRefresh(ctx context.Context, sessionID string, oldRefreshJTI string, newRefreshJTI string, expiresAt time.Time) error
	RevokeSession(ctx context.Context, sessionID string, revokedAt time.Time) error
}

// RevocationStore stores token revocation state in Redis-like backend.
type RevocationStore interface {
	MarkRevoked(ctx context.Context, tokenID string, expiresAt time.Time) error
	IsRevoked(ctx context.Context, tokenID string) (bool, error)
	Ping(ctx context.Context) error
	Close() error
}

// PasswordHasher hashes and verifies user secrets.
type PasswordHasher interface {
	Hash(password string) (string, error)
	Compare(hashedPassword string, password string) error
}

// TokenType distinguishes JWT usage.
type TokenType string

const (
	TokenTypeAccess  TokenType = "access"
	TokenTypeRefresh TokenType = "refresh"
)

// TokenClaims is normalized parsed JWT claims.
type TokenClaims struct {
	TokenID   string
	AccountID string
	SessionID string
	TokenType TokenType
	IssuedAt  time.Time
	ExpiresAt time.Time
}

// IssuedToken contains serialized JWT with parsed metadata.
type IssuedToken struct {
	Raw    string
	Claims TokenClaims
}

// TokenManager handles JWT creation and validation.
type TokenManager interface {
	IssueToken(accountID string, sessionID string, tokenType TokenType, ttl time.Duration, now time.Time) (IssuedToken, error)
	ParseToken(rawToken string, expectedType TokenType) (TokenClaims, error)
}
