package domain

import "time"

// Session tracks refresh lifecycle for account/device pair.
type Session struct {
	ID         string
	AccountID  string
	RefreshJTI string
	UserAgent  string
	IP         string
	CreatedAt  time.Time
	ExpiresAt  time.Time
	RevokedAt  *time.Time
}

func NewSession(id string, accountID string, refreshJTI string, userAgent string, ip string, now time.Time, expiresAt time.Time) (Session, error) {
	if id == "" || accountID == "" || refreshJTI == "" {
		return Session{}, ErrInvalidIdentifier
	}

	if now.IsZero() || expiresAt.IsZero() || !expiresAt.After(now) {
		return Session{}, ErrInvalidTimestamp
	}

	return Session{
		ID:         id,
		AccountID:  accountID,
		RefreshJTI: refreshJTI,
		UserAgent:  userAgent,
		IP:         ip,
		CreatedAt:  now,
		ExpiresAt:  expiresAt,
	}, nil
}

func (s Session) IsRevoked() bool {
	return s.RevokedAt != nil
}

func (s Session) IsExpired(now time.Time) bool {
	return !s.ExpiresAt.After(now)
}

func (s *Session) Revoke(now time.Time) error {
	if now.IsZero() {
		return ErrInvalidTimestamp
	}

	if s.RevokedAt != nil {
		return nil
	}

	revokedAt := now
	s.RevokedAt = &revokedAt

	return nil
}

func (s *Session) RotateRefresh(refreshJTI string) error {
	if refreshJTI == "" {
		return ErrInvalidIdentifier
	}

	s.RefreshJTI = refreshJTI

	return nil
}
