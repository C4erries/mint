package rtcquery

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
)

const (
	defaultTimeout      = 300 * time.Millisecond
	defaultRetryBackoff = 100 * time.Millisecond
)

// DialContextFunc wraps grpc dialer for testability.
type DialContextFunc func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

type Options struct {
	Address      string
	Timeout      time.Duration
	MaxRetries   int
	RetryBackoff time.Duration
	DialOptions  []grpc.DialOption
	DialContext  DialContextFunc
	Client       rtcv1.RTCQueryServiceClient
	Closer       io.Closer
}

// Client wraps RTCQueryService gRPC with timeout/retry policy.
type Client struct {
	client       rtcv1.RTCQueryServiceClient
	closer       io.Closer
	timeout      time.Duration
	maxRetries   int
	retryBackoff time.Duration
}

func NewClient(options Options) (*Client, error) {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	if options.MaxRetries < 0 {
		return nil, fmt.Errorf("rtc max retries must be >= 0")
	}

	retryBackoff := options.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = defaultRetryBackoff
	}

	client := options.Client

	closer := options.Closer
	if client == nil {
		address := strings.TrimSpace(options.Address)
		if address == "" {
			return nil, fmt.Errorf("rtc grpc address is required")
		}

		dialContext := options.DialContext
		if dialContext == nil {
			dialContext = grpc.DialContext
		}

		dialOptions := make([]grpc.DialOption, 0, len(options.DialOptions)+1)

		dialOptions = append(dialOptions, options.DialOptions...)
		if len(dialOptions) == 0 {
			dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		dialCtx, cancel := context.WithTimeout(context.Background(), timeout)
		connection, err := dialContext(dialCtx, address, dialOptions...)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("dial rtc grpc server: %w", err)
		}

		client = rtcv1.NewRTCQueryServiceClient(connection)
		closer = connection
	}

	if client == nil {
		return nil, fmt.Errorf("rtc grpc client is required")
	}

	return &Client{client: client, closer: closer, timeout: timeout, maxRetries: options.MaxRetries, retryBackoff: retryBackoff}, nil
}

func (c *Client) GetVoiceRoomState(ctx context.Context, workspaceID string, channelID string) (*rtcv1.GetVoiceRoomStateResponse, error) {
	var response *rtcv1.GetVoiceRoomStateResponse

	err := c.callWithRetry(ctx, func(callCtx context.Context) error {
		result, err := c.client.GetVoiceRoomState(callCtx, &rtcv1.GetVoiceRoomStateRequest{WorkspaceId: workspaceID, ChannelId: channelID})
		if err != nil {
			return err
		}

		response = result

		return nil
	})

	return response, err
}

func (c *Client) ListVoiceParticipants(ctx context.Context, roomID string) (*rtcv1.ListVoiceParticipantsResponse, error) {
	var response *rtcv1.ListVoiceParticipantsResponse

	err := c.callWithRetry(ctx, func(callCtx context.Context) error {
		result, err := c.client.ListVoiceParticipants(callCtx, &rtcv1.ListVoiceParticipantsRequest{RoomId: roomID})
		if err != nil {
			return err
		}

		response = result

		return nil
	})

	return response, err
}

func (c *Client) GetVoiceChannelBinding(ctx context.Context, workspaceID string, channelID string) (*rtcv1.GetVoiceChannelBindingResponse, error) {
	var response *rtcv1.GetVoiceChannelBindingResponse

	err := c.callWithRetry(ctx, func(callCtx context.Context) error {
		result, err := c.client.GetVoiceChannelBinding(callCtx, &rtcv1.GetVoiceChannelBindingRequest{WorkspaceId: workspaceID, ChannelId: channelID})
		if err != nil {
			return err
		}

		response = result

		return nil
	})

	return response, err
}

func (c *Client) GetRtcTokenGrantStatus(ctx context.Context, tokenID string) (*rtcv1.GetRtcTokenGrantStatusResponse, error) {
	var response *rtcv1.GetRtcTokenGrantStatusResponse

	err := c.callWithRetry(ctx, func(callCtx context.Context) error {
		result, err := c.client.GetRtcTokenGrantStatus(callCtx, &rtcv1.GetRtcTokenGrantStatusRequest{TokenId: tokenID})
		if err != nil {
			return err
		}

		response = result

		return nil
	})

	return response, err
}

func (c *Client) callWithRetry(ctx context.Context, call func(callCtx context.Context) error) error {
	attempts := c.maxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, c.timeout)
		err := call(callCtx)

		cancel()

		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		grpcStatus, ok := status.FromError(err)
		if !ok {
			return err
		}

		switch grpcStatus.Code() {
		case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
			if attempt == attempts {
				return err
			}
		case codes.NotFound, codes.InvalidArgument, codes.PermissionDenied, codes.FailedPrecondition:
			return err
		default:
			return err
		}

		if retryErr := waitForRetry(ctx, c.retryBackoff); retryErr != nil {
			return retryErr
		}
	}

	return nil
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) Close() error {
	if c == nil || c.closer == nil {
		return nil
	}

	return c.closer.Close()
}
