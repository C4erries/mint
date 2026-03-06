package grpcclients

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcHealthV1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	permissionv1 "github.com/c4erries/mint/api/permission/v1"
)

const (
	defaultTimeout      = 300 * time.Millisecond
	defaultRetryBackoff = 100 * time.Millisecond
)

// DialContextFunc wraps grpc dialer construction for testability.
type DialContextFunc func(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error)

// Options configure permission service gRPC client.
type Options struct {
	Address      string
	Timeout      time.Duration
	MaxRetries   int
	RetryBackoff time.Duration
	DialOptions  []grpc.DialOption
	DialContext  DialContextFunc
	Client       permissionv1.PermissionServiceClient
	Closer       io.Closer
}

// PermissionClient checks access policies via remote permission gRPC service.
type PermissionClient struct {
	client       permissionv1.PermissionServiceClient
	healthClient grpcHealthV1.HealthClient
	closer       io.Closer
	timeout      time.Duration
	maxRetries   int
	retryBackoff time.Duration
}

func NewPermissionClient(options Options) (*PermissionClient, error) {
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}

	if options.MaxRetries < 0 {
		return nil, fmt.Errorf("permission max retries must be >= 0")
	}

	retryBackoff := options.RetryBackoff
	if retryBackoff <= 0 {
		retryBackoff = defaultRetryBackoff
	}

	client := options.Client
	closer := options.Closer
	var healthClient grpcHealthV1.HealthClient

	if client == nil {
		address := strings.TrimSpace(options.Address)
		if address == "" {
			return nil, fmt.Errorf("permission grpc address is required")
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

		connection, err := dialContext(context.Background(), address, dialOptions...)
		if err != nil {
			return nil, fmt.Errorf("dial permission grpc server: %w", err)
		}

		client = permissionv1.NewPermissionServiceClient(connection)
		healthClient = grpcHealthV1.NewHealthClient(connection)
		closer = connection
	}

	if client == nil {
		return nil, fmt.Errorf("permission grpc client is required")
	}

	return &PermissionClient{
		client:       client,
		healthClient: healthClient,
		closer:       closer,
		timeout:      timeout,
		maxRetries:   options.MaxRetries,
		retryBackoff: retryBackoff,
	}, nil
}

func (c *PermissionClient) Ping(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("permission client is not initialized")
	}

	if c.healthClient == nil {
		if connection, ok := c.closer.(*grpc.ClientConn); ok {
			c.healthClient = grpcHealthV1.NewHealthClient(connection)
		}
	}

	if c.healthClient == nil {
		return fmt.Errorf("permission health client is not configured")
	}

	response, err := c.healthClient.Check(ctx, &grpcHealthV1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("permission grpc health check failed: %w", err)
	}

	if response.GetStatus() != grpcHealthV1.HealthCheckResponse_SERVING {
		return fmt.Errorf("permission grpc health status is %s", response.GetStatus())
	}

	return nil
}

func (c *PermissionClient) CanJoinVoiceChannel(ctx context.Context, workspaceID string, channelID string, userID string) (bool, error) {
	if workspaceID == "" || channelID == "" || userID == "" {
		return false, fmt.Errorf("workspace/channel/user are required")
	}

	attempts := c.maxRetries + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		allowed, retry, err := c.callOnce(ctx, workspaceID, channelID, userID)
		if err != nil {
			return false, err
		}

		if !retry {
			return allowed, nil
		}

		if attempt == attempts {
			break
		}

		if err = waitForRetry(ctx, c.retryBackoff); err != nil {
			return false, err
		}
	}

	// Fail-closed on transport instability after retries.
	return false, nil
}

func (c *PermissionClient) callOnce(ctx context.Context, workspaceID string, channelID string, userID string) (bool, bool, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	response, err := c.client.CanJoinVoiceChannel(callCtx, &permissionv1.CanJoinVoiceChannelRequest{
		WorkspaceId: workspaceID,
		ChannelId:   channelID,
		UserId:      userID,
	})
	if err == nil {
		if response == nil {
			return false, false, nil
		}

		return response.GetAllowed(), false, nil
	}

	if ctx.Err() != nil {
		return false, false, ctx.Err()
	}

	grpcStatus, ok := status.FromError(err)
	if !ok {
		// Fail-closed for non-gRPC transport errors.
		return false, false, nil
	}

	switch grpcStatus.Code() {
	case codes.PermissionDenied, codes.NotFound:
		return false, false, nil
	case codes.DeadlineExceeded, codes.Unavailable, codes.ResourceExhausted:
		return false, true, nil
	default:
		return false, false, nil
	}
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

func (c *PermissionClient) Close() error {
	if c == nil || c.closer == nil {
		return nil
	}

	return c.closer.Close()
}
