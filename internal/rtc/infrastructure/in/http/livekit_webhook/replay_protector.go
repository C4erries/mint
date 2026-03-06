package livekitwebhook

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
)

// ReplayProtector deduplicates webhook events by event id.
type ReplayProtector interface {
	MarkIfNew(ctx context.Context, eventID string, ttl time.Duration) (bool, error)
}

type inMemoryReplayProtector struct {
	mu      sync.Mutex
	now     func() time.Time
	records map[string]time.Time
}

func NewInMemoryReplayProtector(now func() time.Time) ReplayProtector {
	if now == nil {
		now = time.Now
	}

	return &inMemoryReplayProtector{
		now:     now,
		records: make(map[string]time.Time),
	}
}

func (p *inMemoryReplayProtector) MarkIfNew(ctx context.Context, eventID string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if strings.TrimSpace(eventID) == "" {
		return false, application.ErrInvalidCommand
	}

	now := p.now().UTC()
	expiresAt := now.Add(ttl)

	p.mu.Lock()
	defer p.mu.Unlock()

	for key, value := range p.records {
		if !value.After(now) {
			delete(p.records, key)
		}
	}

	if existing, exists := p.records[eventID]; exists && existing.After(now) {
		return false, nil
	}

	p.records[eventID] = expiresAt

	return true, nil
}
