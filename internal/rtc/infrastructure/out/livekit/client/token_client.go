package livekitclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/pkg/id"
)

// TokenClient issues signed transport-agnostic tokens for RTC access.
type TokenClient struct {
	apiKey      string
	apiSecret   string
	now         func() time.Time
	idGenerator func() string
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
	}
}

func (c *TokenClient) IssueToken(_ context.Context, request application.LiveKitTokenRequest) (application.LiveKitIssuedToken, error) {
	if request.RoomID == "" || request.UserID == "" || request.TTL <= 0 {
		return application.LiveKitIssuedToken{}, application.ErrInvalidCommand
	}

	issuedAt := c.now().UTC()
	expiresAt := issuedAt.Add(request.TTL)
	tokenID := c.idGenerator()

	encodedPayload, err := c.encodePayload(tokenID, request, issuedAt, expiresAt)
	if err != nil {
		return application.LiveKitIssuedToken{}, fmt.Errorf("encode token payload: %w", err)
	}

	token := encodedPayload + "." + c.sign(encodedPayload)

	return application.LiveKitIssuedToken{
		TokenID:   tokenID,
		Token:     token,
		IssuedAt:  issuedAt,
		ExpiresAt: expiresAt,
	}, nil
}

func (c *TokenClient) encodePayload(
	tokenID string,
	request application.LiveKitTokenRequest,
	issuedAt time.Time,
	expiresAt time.Time,
) (string, error) {
	payload := map[string]any{
		"api_key":         c.apiKey,
		"token_id":        tokenID,
		"room_id":         request.RoomID,
		"user_id":         request.UserID,
		"issued_at_unix":  issuedAt.Unix(),
		"expires_at_unix": expiresAt.Unix(),
		"can_publish":     request.CanPublish,
		"can_subscribe":   request.CanSubscribe,
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	return base64.RawURLEncoding.EncodeToString(rawPayload), nil
}

func (c *TokenClient) sign(encodedPayload string) string {
	mac := hmac.New(sha256.New, []byte(c.apiSecret))
	_, _ = mac.Write([]byte(encodedPayload))

	return hex.EncodeToString(mac.Sum(nil))
}
