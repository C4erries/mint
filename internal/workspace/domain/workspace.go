package domain

import (
	"strings"
	"time"
)

// Workspace aggregate root in workspace bounded context.
type Workspace struct {
	ID        string
	Name      string
	OwnerID   string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func NewWorkspace(id string, name string, ownerID string, now time.Time) (Workspace, error) {
	if id == "" || ownerID == "" {
		return Workspace{}, ErrInvalidIdentifier
	}

	trimmedName := strings.TrimSpace(name)
	if trimmedName == "" {
		return Workspace{}, ErrInvalidName
	}

	if now.IsZero() {
		return Workspace{}, ErrInvalidTimestamp
	}

	return Workspace{ID: id, Name: trimmedName, OwnerID: ownerID, CreatedAt: now, UpdatedAt: now}, nil
}
