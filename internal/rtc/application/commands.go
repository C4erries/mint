package application

import (
	"time"

	"github.com/c4erries/mint/internal/rtc/domain"
)

// CommandMeta contains required metadata for idempotent command handling.
type CommandMeta struct {
	CommandID     string    `json:"command_id"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id"`
	MessageID     string    `json:"message_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	WorkspaceID   string    `json:"workspace_id"`
	ChannelID     string    `json:"channel_id"`
	RoomID        string    `json:"room_id"`
	ActorID       string    `json:"actor_id"`
	SchemaVersion int       `json:"schema_version"`
}

func (m CommandMeta) Validate() error {
	if m.CommandID == "" || m.CorrelationID == "" || m.MessageID == "" {
		return ErrInvalidCommand
	}

	if m.WorkspaceID == "" || m.ChannelID == "" || m.ActorID == "" {
		return ErrInvalidCommand
	}

	if m.OccurredAt.IsZero() {
		return ErrInvalidCommand
	}

	if m.SchemaVersion <= 0 {
		return ErrInvalidCommand
	}

	return nil
}

// JoinVoiceChannelCommand opens or reuses room binding and creates active participant session.
type JoinVoiceChannelCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c JoinVoiceChannelCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

// LeaveVoiceChannelCommand closes active participant session.
type LeaveVoiceChannelCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c LeaveVoiceChannelCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

// MuteSelfCommand updates local participant media state.
type MuteSelfCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c MuteSelfCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

// UnmuteSelfCommand updates local participant media state.
type UnmuteSelfCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c UnmuteSelfCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

// EnableCameraCommand updates local participant media state.
type EnableCameraCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c EnableCameraCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

// DisableCameraCommand updates local participant media state.
type DisableCameraCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c DisableCameraCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

// IssueRtcTokenCommand requests short-lived media access grant.
type IssueRtcTokenCommand struct {
	Meta         CommandMeta   `json:"meta"`
	UserID       string        `json:"user_id"`
	TTL          time.Duration `json:"ttl"`
	CanPublish   bool          `json:"can_publish"`
	CanSubscribe bool          `json:"can_subscribe"`
}

func (c IssueRtcTokenCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.UserID == "" {
		return ErrInvalidCommand
	}

	if c.TTL < 0 {
		return ErrInvalidCommand
	}

	return nil
}

// TerminateVoiceSessionCommand force-closes all active sessions in room.
type TerminateVoiceSessionCommand struct {
	Meta CommandMeta `json:"meta"`
}

func (c TerminateVoiceSessionCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	return nil
}

// IncomingCommand contains command type and serialized payload from Kafka envelope.
type IncomingCommand struct {
	Type    string      `json:"type"`
	Meta    CommandMeta `json:"meta"`
	Payload []byte      `json:"-"`
}

func IsCommandTypeSupported(commandType string) bool {
	switch commandType {
	case domain.CommandJoinVoiceChannel,
		domain.CommandLeaveVoiceChannel,
		domain.CommandMuteSelf,
		domain.CommandUnmuteSelf,
		domain.CommandEnableCamera,
		domain.CommandDisableCamera,
		domain.CommandIssueRtcToken,
		domain.CommandTerminateVoiceState:
		return true
	default:
		return false
	}
}
