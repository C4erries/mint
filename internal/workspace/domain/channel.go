package domain

import (
	"strings"
	"time"
)

const (
	ChannelTypeText  = "text"
	ChannelTypeVoice = "voice"
)

// Channel describes workspace channel metadata.
type Channel struct {
	WorkspaceID string
	ID          string
	Name        string
	Kind        string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

func NewChannel(workspaceID string, channelID string, name string, kind string, now time.Time) (Channel, error) {
	if workspaceID == "" || channelID == "" {
		return Channel{}, ErrInvalidIdentifier
	}

	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		return Channel{}, ErrInvalidName
	}

	if kind != ChannelTypeText && kind != ChannelTypeVoice {
		return Channel{}, ErrInvalidName
	}

	if now.IsZero() {
		return Channel{}, ErrInvalidTimestamp
	}

	return Channel{
		WorkspaceID: workspaceID,
		ID:          channelID,
		Name:        trimmedName,
		Kind:        kind,
		CreatedAt:   now,
		UpdatedAt:   now,
	}, nil
}
