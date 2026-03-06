package domain

import "time"

const (
	PermissionJoinVoice = "join_voice"
)

// Role captures permission set and is persisted for future role engine.
type Role struct {
	WorkspaceID string
	ID          string
	Name        string
	Permissions []string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// ChannelOverride stores allow/deny bitmasks for future advanced resolver.
type ChannelOverride struct {
	WorkspaceID string
	ChannelID   string
	SubjectType string
	SubjectID   string
	AllowMask   int64
	DenyMask    int64
	UpdatedAt   time.Time
}

// PermissionSnapshot is neutral data contract for permission evaluator.
type PermissionSnapshot struct {
	WorkspaceID      string
	ChannelID        string
	Members          map[string]Member
	RolesByID        map[string]Role
	ChannelOverrides []ChannelOverride
}

// HasExplicitJoinVoiceDeny checks whether user/channel override denies voice join.
func (s PermissionSnapshot) HasExplicitJoinVoiceDeny(userID string) bool {
	for _, override := range s.ChannelOverrides {
		if override.SubjectType != "user" || override.SubjectID != userID {
			continue
		}

		if override.DenyMask&JoinVoiceMask != 0 {
			return true
		}
	}

	return false
}

// HasExplicitJoinVoiceAllow checks whether user/channel override allows voice join.
func (s PermissionSnapshot) HasExplicitJoinVoiceAllow(userID string) bool {
	for _, override := range s.ChannelOverrides {
		if override.SubjectType != "user" || override.SubjectID != userID {
			continue
		}

		if override.AllowMask&JoinVoiceMask != 0 {
			return true
		}
	}

	return false
}

const (
	// JoinVoiceMask reserved for future full role/override evaluator.
	JoinVoiceMask int64 = 1 << 0
)
