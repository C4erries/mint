package domain

import "errors"

var (
	ErrInvalidIdentifier           = errors.New("rtc domain: invalid identifier")
	ErrInvalidTimestamp            = errors.New("rtc domain: invalid timestamp")
	ErrVoiceRoomNotFound           = errors.New("rtc domain: voice room not found")
	ErrVoiceChannelBindingNotFound = errors.New("rtc domain: voice channel binding not found")
	ErrParticipantAlreadyJoined    = errors.New("rtc domain: participant already joined")
	ErrParticipantNotFound         = errors.New("rtc domain: participant not found")
	ErrRoomInactive                = errors.New("rtc domain: room is inactive")
	ErrGrantNotFound               = errors.New("rtc domain: media access grant not found")
	ErrGrantExpired                = errors.New("rtc domain: media access grant expired")
	ErrGrantAlreadyExists          = errors.New("rtc domain: media access grant already exists")
)
