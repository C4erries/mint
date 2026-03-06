package domain

import "time"

// VoiceChannelBinding links a workspace voice channel to a concrete room.
type VoiceChannelBinding struct {
	WorkspaceID string
	ChannelID   string
	RoomID      string
	BoundAt     time.Time
	UpdatedAt   time.Time
}

func NewVoiceChannelBinding(workspaceID string, channelID string, roomID string, now time.Time) (*VoiceChannelBinding, error) {
	if workspaceID == "" || channelID == "" || roomID == "" {
		return nil, ErrInvalidIdentifier
	}

	if now.IsZero() {
		return nil, ErrInvalidTimestamp
	}

	return &VoiceChannelBinding{
		WorkspaceID: workspaceID,
		ChannelID:   channelID,
		RoomID:      roomID,
		BoundAt:     now,
		UpdatedAt:   now,
	}, nil
}

func (b *VoiceChannelBinding) Rebind(roomID string, now time.Time) error {
	if roomID == "" {
		return ErrInvalidIdentifier
	}

	if now.IsZero() {
		return ErrInvalidTimestamp
	}

	b.RoomID = roomID
	b.UpdatedAt = now

	return nil
}

func (b *VoiceChannelBinding) Clone() *VoiceChannelBinding {
	return &VoiceChannelBinding{
		WorkspaceID: b.WorkspaceID,
		ChannelID:   b.ChannelID,
		RoomID:      b.RoomID,
		BoundAt:     b.BoundAt,
		UpdatedAt:   b.UpdatedAt,
	}
}
