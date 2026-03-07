package application

import "time"

// OutboxMessage is the canonical workspace event envelope.
type OutboxMessage struct {
	EventID       string            `json:"event_id"`
	EventType     string            `json:"event_type"`
	CommandID     string            `json:"command_id"`
	CorrelationID string            `json:"correlation_id"`
	CausationID   string            `json:"causation_id"`
	MessageID     string            `json:"message_id"`
	OccurredAt    time.Time         `json:"occurred_at"`
	WorkspaceID   string            `json:"workspace_id"`
	ChannelID     string            `json:"channel_id"`
	ActorID       string            `json:"actor_id"`
	SchemaVersion int               `json:"schema_version"`
	Payload       map[string]string `json:"payload"`
}
