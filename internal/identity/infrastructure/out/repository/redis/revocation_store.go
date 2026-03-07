package redisrepo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultKeyPrefix = "mint:core"

// RedisClient narrows go-redis API surface for token revocation.
type RedisClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
	Exists(ctx context.Context, keys ...string) *redis.IntCmd
	Close() error
}

// ClientFactory wraps redis.NewClient for testing.
type ClientFactory interface {
	NewClient(options *redis.Options) RedisClient
}

type defaultClientFactory struct{}

func (defaultClientFactory) NewClient(options *redis.Options) RedisClient {
	return redis.NewClient(options)
}

// RevocationStore is Redis-backed token revocation storage.
type RevocationStore struct {
	client    RedisClient
	keyPrefix string
}

type Options struct {
	Addr          string
	Password      string
	DB            int
	KeyPrefix     string
	ClientFactory ClientFactory
}

func NewRevocationStore(options Options) (*RevocationStore, error) {
	if strings.TrimSpace(options.Addr) == "" {
		return nil, fmt.Errorf("redis address is required")
	}

	if options.KeyPrefix == "" {
		options.KeyPrefix = defaultKeyPrefix
	}

	if options.ClientFactory == nil {
		options.ClientFactory = defaultClientFactory{}
	}

	client := options.ClientFactory.NewClient(&redis.Options{
		Addr:     strings.TrimSpace(options.Addr),
		Password: options.Password,
		DB:       options.DB,
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &RevocationStore{client: client, keyPrefix: options.KeyPrefix}, nil
}

func (s *RevocationStore) MarkRevoked(ctx context.Context, tokenID string, expiresAt time.Time) error {
	if tokenID == "" {
		return fmt.Errorf("token id is required")
	}

	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return nil
	}

	if err := s.client.Set(ctx, s.key(tokenID), "1", ttl).Err(); err != nil {
		return fmt.Errorf("set revoked token key: %w", err)
	}

	return nil
}

func (s *RevocationStore) IsRevoked(ctx context.Context, tokenID string) (bool, error) {
	if tokenID == "" {
		return false, fmt.Errorf("token id is required")
	}

	exists, err := s.client.Exists(ctx, s.key(tokenID)).Result()
	if err != nil {
		return false, fmt.Errorf("check revoked token key: %w", err)
	}

	return exists > 0, nil
}

func (s *RevocationStore) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis revocation client is not initialized")
	}

	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}

	return nil
}

func (s *RevocationStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}

	return s.client.Close()
}

func (s *RevocationStore) key(tokenID string) string {
	return fmt.Sprintf("%s:identity:revoked:%s", s.keyPrefix, tokenID)
}
