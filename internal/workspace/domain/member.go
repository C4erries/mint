package domain

import "time"

// Member tracks workspace membership and moderation state.
type Member struct {
	WorkspaceID string
	UserID      string
	JoinedAt    time.Time
	Banned      bool
	RoleIDs     []string
}

func NewMember(workspaceID string, userID string, now time.Time) (Member, error) {
	if workspaceID == "" || userID == "" {
		return Member{}, ErrInvalidIdentifier
	}

	if now.IsZero() {
		return Member{}, ErrInvalidTimestamp
	}

	return Member{
		WorkspaceID: workspaceID,
		UserID:      userID,
		JoinedAt:    now,
		Banned:      false,
		RoleIDs:     make([]string, 0),
	}, nil
}

func (m *Member) Ban() {
	m.Banned = true
}
