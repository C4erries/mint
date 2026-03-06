package application

import "time"

// WorkspaceView is read projection optimized for API responses.
type WorkspaceView struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	OwnerID   string    `json:"owner_id"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ChannelView is read projection for channel metadata.
type ChannelView struct {
	WorkspaceID string    `json:"workspace_id"`
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Kind        string    `json:"kind"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type GetWorkspaceQuery struct {
	WorkspaceID string
}

func (q GetWorkspaceQuery) Validate() error {
	if q.WorkspaceID == "" {
		return ErrInvalidQuery
	}

	return nil
}

type GetChannelQuery struct {
	WorkspaceID string
	ChannelID   string
}

func (q GetChannelQuery) Validate() error {
	if q.WorkspaceID == "" || q.ChannelID == "" {
		return ErrInvalidQuery
	}

	return nil
}
