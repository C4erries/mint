package grpcclients

import (
	"context"
	"sync"
)

// PermissionClient emulates synchronous gRPC policy checks from external contexts.
type PermissionClient struct {
	mu sync.RWMutex

	denied map[string]struct{}
}

func NewPermissionClient() *PermissionClient {
	return &PermissionClient{
		denied: make(map[string]struct{}),
	}
}

func (c *PermissionClient) CanJoinVoiceChannel(_ context.Context, workspaceID string, channelID string, userID string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, denied := c.denied[key(workspaceID, channelID, userID)]
	return !denied, nil
}

func (c *PermissionClient) Deny(workspaceID string, channelID string, userID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.denied[key(workspaceID, channelID, userID)] = struct{}{}
}

func key(workspaceID string, channelID string, userID string) string {
	return workspaceID + ":" + channelID + ":" + userID
}
