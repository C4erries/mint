package livekitclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// RoomServiceTokenClient defines minimal token creation contract exposed by LiveKit SDK.
type RoomServiceTokenClient interface {
	CreateToken() *auth.AccessToken
}

// RoomServiceClientFactory wraps SDK room service constructor for testability.
type RoomServiceClientFactory interface {
	NewRoomServiceClient(hostURL string, apiKey string, apiSecret string) RoomServiceTokenClient
}

type defaultRoomServiceClientFactory struct{}

func (defaultRoomServiceClientFactory) NewRoomServiceClient(hostURL string, apiKey string, apiSecret string) RoomServiceTokenClient {
	return lksdk.NewRoomServiceClient(hostURL, apiKey, apiSecret)
}

// Options configure TokenClient construction.
type Options struct {
	HostURL       string
	APIKey        string
	APISecret     string
	Now           func() time.Time
	IDGenerator   func() string
	ClientFactory RoomServiceClientFactory
}

// TokenClient issues LiveKit JWT tokens via official LiveKit Go SDK.
type TokenClient struct {
	now         func() time.Time
	idGenerator func() string
	roomClient  RoomServiceTokenClient
}

func NewTokenClient(options Options) (*TokenClient, error) {
	if strings.TrimSpace(options.HostURL) == "" {
		return nil, fmt.Errorf("livekit host url is required")
	}

	if strings.TrimSpace(options.APIKey) == "" {
		return nil, fmt.Errorf("livekit api key is required")
	}

	if strings.TrimSpace(options.APISecret) == "" {
		return nil, fmt.Errorf("livekit api secret is required")
	}

	if options.Now == nil {
		options.Now = time.Now
	}

	if options.IDGenerator == nil {
		options.IDGenerator = uuid.NewString
	}

	if options.ClientFactory == nil {
		options.ClientFactory = defaultRoomServiceClientFactory{}
	}

	roomClient := options.ClientFactory.NewRoomServiceClient(options.HostURL, options.APIKey, options.APISecret)
	if roomClient == nil {
		return nil, fmt.Errorf("livekit room service client is required")
	}

	return &TokenClient{
		now:         options.Now,
		idGenerator: options.IDGenerator,
		roomClient:  roomClient,
	}, nil
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

	token, err := c.roomClient.CreateToken().
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
