package application

import "time"

// VoiceRoomState represents read model optimized for room state view.
type VoiceRoomState struct {
	RoomID            string    `json:"room_id"`
	WorkspaceID       string    `json:"workspace_id"`
	ChannelID         string    `json:"channel_id"`
	Active            bool      `json:"active"`
	ParticipantCount  int       `json:"participant_count"`
	LastStateChangeAt time.Time `json:"last_state_change_at"`
}

// VoiceParticipant represents read model entry for participant listing.
type VoiceParticipant struct {
	UserID          string     `json:"user_id"`
	JoinedAt        time.Time  `json:"joined_at"`
	LeftAt          *time.Time `json:"left_at,omitempty"`
	MicrophoneMuted bool       `json:"microphone_muted"`
	CameraEnabled   bool       `json:"camera_enabled"`
}

// VoiceChannelBindingView represents read model for channel-room mapping.
type VoiceChannelBindingView struct {
	WorkspaceID string    `json:"workspace_id"`
	ChannelID   string    `json:"channel_id"`
	RoomID      string    `json:"room_id"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// RtcTokenGrantStatus represents read model for issued media token.
type RtcTokenGrantStatus struct {
	TokenID      string    `json:"token_id"`
	RoomID       string    `json:"room_id"`
	UserID       string    `json:"user_id"`
	IssuedAt     time.Time `json:"issued_at"`
	ExpiresAt    time.Time `json:"expires_at"`
	Expired      bool      `json:"expired"`
	CanPublish   bool      `json:"can_publish"`
	CanSubscribe bool      `json:"can_subscribe"`
	Token        string    `json:"token"`
}

// Query DTOs.
type GetVoiceRoomStateQuery struct {
	WorkspaceID string
	ChannelID   string
}

type ListVoiceParticipantsQuery struct {
	RoomID string
}

type GetVoiceChannelBindingQuery struct {
	WorkspaceID string
	ChannelID   string
}

type GetRtcTokenGrantStatusQuery struct {
	TokenID string
}

func (q GetVoiceRoomStateQuery) Validate() error {
	if q.WorkspaceID == "" || q.ChannelID == "" {
		return ErrInvalidQuery
	}

	return nil
}

func (q ListVoiceParticipantsQuery) Validate() error {
	if q.RoomID == "" {
		return ErrInvalidQuery
	}

	return nil
}

func (q GetVoiceChannelBindingQuery) Validate() error {
	if q.WorkspaceID == "" || q.ChannelID == "" {
		return ErrInvalidQuery
	}

	return nil
}

func (q GetRtcTokenGrantStatusQuery) Validate() error {
	if q.TokenID == "" {
		return ErrInvalidQuery
	}

	return nil
}
