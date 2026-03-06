package domain

import "time"

// VoiceParticipantSession stores the participant media state in application terms.
type VoiceParticipantSession struct {
	UserID          string
	JoinedAt        time.Time
	LeftAt          *time.Time
	MicrophoneMuted bool
	CameraEnabled   bool
}

func (s VoiceParticipantSession) isActive() bool {
	return s.LeftAt == nil
}

func (s VoiceParticipantSession) clone() VoiceParticipantSession {
	copySession := s

	if s.LeftAt != nil {
		leftAt := *s.LeftAt
		copySession.LeftAt = &leftAt
	}

	return copySession
}

// VoiceRoom is the aggregate root for voice/video participation state.
type VoiceRoom struct {
	ID          string
	WorkspaceID string
	ChannelID   string
	Active      bool
	CreatedAt   time.Time
	UpdatedAt   time.Time

	participants map[string]VoiceParticipantSession
}

func NewVoiceRoom(id string, workspaceID string, channelID string, now time.Time) (*VoiceRoom, error) {
	if id == "" || workspaceID == "" || channelID == "" {
		return nil, ErrInvalidIdentifier
	}

	if now.IsZero() {
		return nil, ErrInvalidTimestamp
	}

	return &VoiceRoom{
		ID:           id,
		WorkspaceID:  workspaceID,
		ChannelID:    channelID,
		Active:       true,
		CreatedAt:    now,
		UpdatedAt:    now,
		participants: make(map[string]VoiceParticipantSession),
	}, nil
}

func (r *VoiceRoom) JoinParticipant(userID string, now time.Time) (VoiceParticipantSession, error) {
	if userID == "" {
		return VoiceParticipantSession{}, ErrInvalidIdentifier
	}

	if now.IsZero() {
		return VoiceParticipantSession{}, ErrInvalidTimestamp
	}

	if !r.Active {
		return VoiceParticipantSession{}, ErrRoomInactive
	}

	participant, found := r.participants[userID]
	if found && participant.isActive() {
		return VoiceParticipantSession{}, ErrParticipantAlreadyJoined
	}

	r.participants[userID] = VoiceParticipantSession{
		UserID:          userID,
		JoinedAt:        now,
		LeftAt:          nil,
		MicrophoneMuted: false,
		CameraEnabled:   false,
	}

	r.UpdatedAt = now

	return r.participants[userID].clone(), nil
}

func (r *VoiceRoom) LeaveParticipant(userID string, now time.Time) (VoiceParticipantSession, error) {
	if userID == "" {
		return VoiceParticipantSession{}, ErrInvalidIdentifier
	}

	if now.IsZero() {
		return VoiceParticipantSession{}, ErrInvalidTimestamp
	}

	participant, found := r.participants[userID]
	if !found || !participant.isActive() {
		return VoiceParticipantSession{}, ErrParticipantNotFound
	}

	participant.LeftAt = &now
	r.participants[userID] = participant
	r.UpdatedAt = now

	if r.ActiveParticipantCount() == 0 {
		r.Active = false
	}

	return participant.clone(), nil
}

func (r *VoiceRoom) SetMicrophoneMuted(userID string, muted bool, now time.Time) (VoiceParticipantSession, error) {
	participant, err := r.getActiveParticipant(userID)
	if err != nil {
		return VoiceParticipantSession{}, err
	}

	if now.IsZero() {
		return VoiceParticipantSession{}, ErrInvalidTimestamp
	}

	participant.MicrophoneMuted = muted
	r.participants[userID] = participant
	r.UpdatedAt = now

	return participant.clone(), nil
}

func (r *VoiceRoom) SetCameraEnabled(userID string, enabled bool, now time.Time) (VoiceParticipantSession, error) {
	participant, err := r.getActiveParticipant(userID)
	if err != nil {
		return VoiceParticipantSession{}, err
	}

	if now.IsZero() {
		return VoiceParticipantSession{}, ErrInvalidTimestamp
	}

	participant.CameraEnabled = enabled
	r.participants[userID] = participant
	r.UpdatedAt = now

	return participant.clone(), nil
}

func (r *VoiceRoom) Terminate(now time.Time) error {
	if now.IsZero() {
		return ErrInvalidTimestamp
	}

	for userID, participant := range r.participants {
		if !participant.isActive() {
			continue
		}

		leftAt := now
		participant.LeftAt = &leftAt
		r.participants[userID] = participant
	}

	r.Active = false
	r.UpdatedAt = now

	return nil
}

func (r *VoiceRoom) Touch(now time.Time) error {
	if now.IsZero() {
		return ErrInvalidTimestamp
	}

	r.UpdatedAt = now

	return nil
}

func (r *VoiceRoom) Participant(userID string) (VoiceParticipantSession, bool) {
	participant, found := r.participants[userID]
	if !found {
		return VoiceParticipantSession{}, false
	}

	return participant.clone(), true
}

func (r *VoiceRoom) ActiveParticipantCount() int {
	count := 0

	for _, participant := range r.participants {
		if participant.isActive() {
			count++
		}
	}

	return count
}

func (r *VoiceRoom) Participants() []VoiceParticipantSession {
	result := make([]VoiceParticipantSession, 0, len(r.participants))

	for _, participant := range r.participants {
		result = append(result, participant.clone())
	}

	return result
}

func (r *VoiceRoom) Clone() *VoiceRoom {
	copied := &VoiceRoom{
		ID:           r.ID,
		WorkspaceID:  r.WorkspaceID,
		ChannelID:    r.ChannelID,
		Active:       r.Active,
		CreatedAt:    r.CreatedAt,
		UpdatedAt:    r.UpdatedAt,
		participants: make(map[string]VoiceParticipantSession, len(r.participants)),
	}

	for userID, participant := range r.participants {
		copied.participants[userID] = participant.clone()
	}

	return copied
}

func (r *VoiceRoom) getActiveParticipant(userID string) (VoiceParticipantSession, error) {
	if userID == "" {
		return VoiceParticipantSession{}, ErrInvalidIdentifier
	}

	participant, found := r.participants[userID]
	if !found || !participant.isActive() {
		return VoiceParticipantSession{}, ErrParticipantNotFound
	}

	return participant, nil
}
