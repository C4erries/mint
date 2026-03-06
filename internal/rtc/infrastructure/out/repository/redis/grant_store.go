package redisrepo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/c4erries/mint/internal/rtc/domain"
	"github.com/redis/go-redis/v9"
)

const defaultKeyPrefix = "mint:rtc"

// RedisClient narrows go-redis API surface used by grant repository.
type RedisClient interface {
	Ping(ctx context.Context) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd
	HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Close() error
}

// ClientFactory wraps redis.NewClient for testability.
type ClientFactory interface {
	NewClient(options *redis.Options) RedisClient
}

type defaultClientFactory struct{}

func (defaultClientFactory) NewClient(options *redis.Options) RedisClient {
	return redis.NewClient(options)
}

// Options contains Redis connection parameters for grants store.
type Options struct {
	Addr          string
	Password      string
	DB            int
	KeyPrefix     string
	Now           func() time.Time
	ClientFactory ClientFactory
}

// GrantStore is a Redis-backed storage for short-lived media access grants.
type GrantStore struct {
	client    RedisClient
	now       func() time.Time
	keyPrefix string
}

func NewGrantStore(options Options) (*GrantStore, error) {
	if strings.TrimSpace(options.Addr) == "" {
		return nil, fmt.Errorf("redis address is required")
	}

	if options.Now == nil {
		options.Now = time.Now
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

	return &GrantStore{client: client, now: options.Now, keyPrefix: options.KeyPrefix}, nil
}

func (s *GrantStore) SaveGrant(ctx context.Context, grant domain.MediaAccessGrant) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	ttl := time.Until(grant.ExpiresAt)
	if ttl <= 0 {
		return domain.ErrGrantExpired
	}

	commandKey := s.commandKey(grant.CommandID)
	created, err := s.client.SetNX(ctx, commandKey, grant.TokenID, ttl).Result()
	if err != nil {
		return fmt.Errorf("reserve grant command key: %w", err)
	}

	if !created {
		return domain.ErrGrantAlreadyExists
	}

	grantKey := s.grantKey(grant.TokenID)
	if err = s.client.HSet(ctx, grantKey, map[string]interface{}{
		"token_id":      grant.TokenID,
		"command_id":    grant.CommandID,
		"room_id":       grant.RoomID,
		"user_id":       grant.UserID,
		"token":         grant.Token,
		"can_publish":   grant.CanPublish,
		"can_subscribe": grant.CanSubscribe,
		"issued_at":     grant.IssuedAt.UTC().Format(time.RFC3339Nano),
		"expires_at":    grant.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}).Err(); err != nil {
		_ = s.client.Del(ctx, commandKey).Err()
		return fmt.Errorf("save grant hash: %w", err)
	}

	if err = s.client.Expire(ctx, grantKey, ttl).Err(); err != nil {
		_ = s.client.Del(ctx, commandKey, grantKey).Err()
		return fmt.Errorf("set grant ttl: %w", err)
	}

	return nil
}

func (s *GrantStore) GetGrant(ctx context.Context, tokenID string) (domain.MediaAccessGrant, error) {
	if err := ctx.Err(); err != nil {
		return domain.MediaAccessGrant{}, err
	}

	values, err := s.client.HGetAll(ctx, s.grantKey(tokenID)).Result()
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("read grant hash: %w", err)
	}

	if len(values) == 0 {
		return domain.MediaAccessGrant{}, domain.ErrGrantNotFound
	}

	grant, err := decodeGrant(values)
	if err != nil {
		return domain.MediaAccessGrant{}, err
	}

	if grant.IsExpired(s.now()) {
		return domain.MediaAccessGrant{}, domain.ErrGrantExpired
	}

	return grant, nil
}

func (s *GrantStore) GetGrantByCommandID(ctx context.Context, commandID string) (domain.MediaAccessGrant, error) {
	if err := ctx.Err(); err != nil {
		return domain.MediaAccessGrant{}, err
	}

	tokenID, err := s.client.Get(ctx, s.commandKey(commandID)).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return domain.MediaAccessGrant{}, domain.ErrGrantNotFound
		}

		return domain.MediaAccessGrant{}, fmt.Errorf("read command grant index: %w", err)
	}

	return s.GetGrant(ctx, tokenID)
}

func (s *GrantStore) Ping(ctx context.Context) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis client is not initialized")
	}

	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}

	return nil
}

func (s *GrantStore) Close() error {
	if s == nil || s.client == nil {
		return nil
	}

	return s.client.Close()
}

func (s *GrantStore) grantKey(tokenID string) string {
	return fmt.Sprintf("%s:grant:%s", s.keyPrefix, tokenID)
}

func (s *GrantStore) commandKey(commandID string) string {
	return fmt.Sprintf("%s:grant:command:%s", s.keyPrefix, commandID)
}

func decodeGrant(values map[string]string) (domain.MediaAccessGrant, error) {
	canPublish, err := strconv.ParseBool(values["can_publish"])
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("parse can_publish: %w", err)
	}

	canSubscribe, err := strconv.ParseBool(values["can_subscribe"])
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("parse can_subscribe: %w", err)
	}

	issuedAt, err := time.Parse(time.RFC3339Nano, values["issued_at"])
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("parse issued_at: %w", err)
	}

	expiresAt, err := time.Parse(time.RFC3339Nano, values["expires_at"])
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("parse expires_at: %w", err)
	}

	grant, err := domain.NewMediaAccessGrant(
		values["token_id"],
		values["command_id"],
		values["room_id"],
		values["user_id"],
		values["token"],
		canPublish,
		canSubscribe,
		issuedAt,
		expiresAt,
	)
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("build grant from redis: %w", err)
	}

	return grant, nil
}
