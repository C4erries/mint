package application

import "time"

// CommandMeta contains metadata for idempotent command handling.
type CommandMeta struct {
	CommandID     string    `json:"command_id"`
	CorrelationID string    `json:"correlation_id"`
	CausationID   string    `json:"causation_id"`
	MessageID     string    `json:"message_id"`
	OccurredAt    time.Time `json:"occurred_at"`
	WorkspaceID   string    `json:"workspace_id"`
	ChannelID     string    `json:"channel_id"`
	ActorID       string    `json:"actor_id"`
	SchemaVersion int       `json:"schema_version"`
}

func (m CommandMeta) Validate() error {
	if m.CommandID == "" || m.CorrelationID == "" || m.MessageID == "" || m.ActorID == "" {
		return ErrInvalidCommand
	}

	if m.OccurredAt.IsZero() || m.SchemaVersion <= 0 {
		return ErrInvalidCommand
	}

	return nil
}

type CreateWorkspaceCommand struct {
	Meta          CommandMeta `json:"meta"`
	WorkspaceName string      `json:"workspace_name"`
}

func (c CreateWorkspaceCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.Meta.WorkspaceID == "" || c.WorkspaceName == "" {
		return ErrInvalidCommand
	}

	return nil
}

type CreateChannelCommand struct {
	Meta        CommandMeta `json:"meta"`
	ChannelName string      `json:"channel_name"`
	ChannelKind string      `json:"channel_kind"`
}

func (c CreateChannelCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.Meta.WorkspaceID == "" || c.Meta.ChannelID == "" || c.ChannelName == "" || c.ChannelKind == "" {
		return ErrInvalidCommand
	}

	return nil
}

type JoinWorkspaceCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c JoinWorkspaceCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.Meta.WorkspaceID == "" || c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

type BanMemberCommand struct {
	Meta   CommandMeta `json:"meta"`
	UserID string      `json:"user_id"`
}

func (c BanMemberCommand) Validate() error {
	if err := c.Meta.Validate(); err != nil {
		return err
	}

	if c.Meta.WorkspaceID == "" || c.UserID == "" {
		return ErrInvalidCommand
	}

	return nil
}

func IsCommandTypeSupported(commandType string) bool {
	switch commandType {
	case "CreateWorkspace", "CreateChannel", "JoinWorkspace", "BanMember":
		return true
	default:
		return false
	}
}
