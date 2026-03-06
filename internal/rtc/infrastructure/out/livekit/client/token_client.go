package livekitclient

import (
	"context"
	"fmt"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/pkg/id"
	"github.com/livekit/protocol/auth"
)

// AccessTokenBuilder is a test-friendly wrapper around livekit auth token builder.
type AccessTokenBuilder interface {
	SetIdentity(identity string) AccessTokenBuilder
	SetVideoGrant(grant *auth.VideoGrant) AccessTokenBuilder
	SetValidFor(duration time.Duration) AccessTokenBuilder
	ToJWT() (string, error)
}

// AccessTokenFactory wraps livekit token constructor for easier testing.
type AccessTokenFactory interface {
	NewAccessToken(apiKey string, apiSecret string) AccessTokenBuilder
}

type defaultAccessTokenFactory struct{}

func (defaultAccessTokenFactory) NewAccessToken(apiKey string, apiSecret string) AccessTokenBuilder {
	return &accessTokenAdapter{token: auth.NewAccessToken(apiKey, apiSecret)}
}

type accessTokenAdapter struct {
	token *auth.AccessToken
}

func (a *accessTokenAdapter) SetIdentity(identity string) AccessTokenBuilder {
	a.token.SetIdentity(identity)
	return a
}

func (a *accessTokenAdapter) SetVideoGrant(grant *auth.VideoGrant) AccessTokenBuilder {
	a.token.SetVideoGrant(grant)
	return a
}

func (a *accessTokenAdapter) SetValidFor(duration time.Duration) AccessTokenBuilder {
	a.token.SetValidFor(duration)
	return a
}

func (a *accessTokenAdapter) ToJWT() (string, error) {
	return a.token.ToJWT()
}

// TokenClient issues LiveKit JWT tokens via official protocol library.
type TokenClient struct {
	apiKey      string
	apiSecret   string
	now         func() time.Time
	idGenerator func() string
	factory     AccessTokenFactory
}

func NewTokenClient(apiKey string, apiSecret string, now func() time.Time) *TokenClient {
	if now == nil {
		now = time.Now
	}

	return &TokenClient{
		apiKey:      apiKey,
		apiSecret:   apiSecret,
		now:         now,
		idGenerator: id.New,
		factory:     defaultAccessTokenFactory{},
	}
}

func (c *TokenClient) IssueToken(_ context.Context, request application.LiveKitTokenRequest) (application.LiveKitIssuedToken, error) {
	if request.RoomID == "" || request.UserID == "" || request.TTL <= 0 {
		return application.LiveKitIssuedToken{}, application.ErrInvalidCommand
	}

	issuedAt := c.now().UTC()
	expiresAt := issuedAt.Add(request.TTL)
	tokenID := c.idGenerator()

	grant := &auth.VideoGrant{RoomJoin: true, Room: request.RoomID}
	grant.SetCanPublish(request.CanPublish)
	grant.SetCanSubscribe(request.CanSubscribe)

	token, err := c.factory.NewAccessToken(c.apiKey, c.apiSecret).
		SetIdentity(request.UserID).
		SetVideoGrant(grant).
		SetValidFor(request.TTL).
		ToJWT()
	if err != nil {
		return application.LiveKitIssuedToken{}, fmt.Errorf("build livekit jwt: %w", err)
	}

	return application.LiveKitIssuedToken{
		TokenID:   tokenID,
		Token:     token,
		IssuedAt:  issuedAt,
		ExpiresAt: expiresAt,
	}, nil
}
