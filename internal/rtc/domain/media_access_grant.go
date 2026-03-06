package domain

import "time"

// MediaAccessGrant represents a short-lived access grant used for joining a media room.
type MediaAccessGrant struct {
	TokenID      string
	CommandID    string
	RoomID       string
	UserID       string
	Token        string
	CanPublish   bool
	CanSubscribe bool
	IssuedAt     time.Time
	ExpiresAt    time.Time
}

func NewMediaAccessGrant(tokenID string, commandID string, roomID string, userID string, token string, canPublish bool, canSubscribe bool, issuedAt time.Time, expiresAt time.Time) (MediaAccessGrant, error) {
	if tokenID == "" || commandID == "" || roomID == "" || userID == "" || token == "" {
		return MediaAccessGrant{}, ErrInvalidIdentifier
	}

	if issuedAt.IsZero() || expiresAt.IsZero() {
		return MediaAccessGrant{}, ErrInvalidTimestamp
	}

	if !expiresAt.After(issuedAt) {
		return MediaAccessGrant{}, ErrInvalidTimestamp
	}

	return MediaAccessGrant{
		TokenID:      tokenID,
		CommandID:    commandID,
		RoomID:       roomID,
		UserID:       userID,
		Token:        token,
		CanPublish:   canPublish,
		CanSubscribe: canSubscribe,
		IssuedAt:     issuedAt,
		ExpiresAt:    expiresAt,
	}, nil
}

func (g MediaAccessGrant) IsExpired(now time.Time) bool {
	return !now.Before(g.ExpiresAt)
}
