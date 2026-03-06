package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/c4erries/mint/internal/rtc/domain"
)

// GrantStore is an in-memory Redis-like storage for short-lived media access grants.
type GrantStore struct {
	mu sync.RWMutex

	now          func() time.Time
	grants       map[string]domain.MediaAccessGrant
	commandIndex map[string]string
}

func NewGrantStore(now func() time.Time) *GrantStore {
	if now == nil {
		now = time.Now
	}

	return &GrantStore{
		now:          now,
		grants:       make(map[string]domain.MediaAccessGrant),
		commandIndex: make(map[string]string),
	}
}

func (s *GrantStore) SaveGrant(ctx context.Context, grant domain.MediaAccessGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.grants[grant.TokenID]; exists {
		return domain.ErrGrantAlreadyExists
	}

	if _, exists := s.commandIndex[grant.CommandID]; exists {
		return domain.ErrGrantAlreadyExists
	}

	s.grants[grant.TokenID] = grant
	s.commandIndex[grant.CommandID] = grant.TokenID

	return nil
}

func (s *GrantStore) GetGrant(ctx context.Context, tokenID string) (domain.MediaAccessGrant, error) {
	if err := ctx.Err(); err != nil {
		return domain.MediaAccessGrant{}, err
	}

	s.mu.RLock()
	grant, exists := s.grants[tokenID]
	s.mu.RUnlock()
	if !exists {
		return domain.MediaAccessGrant{}, domain.ErrGrantNotFound
	}

	if grant.IsExpired(s.now()) {
		return domain.MediaAccessGrant{}, domain.ErrGrantExpired
	}

	return grant, nil
}
