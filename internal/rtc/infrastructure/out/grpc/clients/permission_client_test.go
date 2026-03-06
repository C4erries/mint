package grpcclients

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	permissionv1 "github.com/c4erries/mint/api/permission/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestPermissionClient_CanJoinVoiceChannel_Allowed(t *testing.T) {
	t.Parallel()

	client := newTestPermissionClient(t, func(_ context.Context, _ *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
		return &permissionv1.CanJoinVoiceChannelResponse{Allowed: true}, nil
	}, 50*time.Millisecond, 0, time.Millisecond)

	allowed, err := client.CanJoinVoiceChannel(context.Background(), "ws", "ch", "user")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestPermissionClient_CanJoinVoiceChannel_PermissionDeniedIsFailClosed(t *testing.T) {
	t.Parallel()

	client := newTestPermissionClient(t, func(_ context.Context, _ *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "denied")
	}, 50*time.Millisecond, 0, time.Millisecond)

	allowed, err := client.CanJoinVoiceChannel(context.Background(), "ws", "ch", "user")
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestPermissionClient_CanJoinVoiceChannel_NotFoundIsFailClosed(t *testing.T) {
	t.Parallel()

	client := newTestPermissionClient(t, func(_ context.Context, _ *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
		return nil, status.Error(codes.NotFound, "missing")
	}, 50*time.Millisecond, 0, time.Millisecond)

	allowed, err := client.CanJoinVoiceChannel(context.Background(), "ws", "ch", "user")
	require.NoError(t, err)
	require.False(t, allowed)
}

func TestPermissionClient_CanJoinVoiceChannel_TimeoutRetriesThenFailClosed(t *testing.T) {
	t.Parallel()

	var calls int32
	client := newTestPermissionClient(t, func(ctx context.Context, _ *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
		atomic.AddInt32(&calls, 1)
		<-ctx.Done()
		return nil, status.Error(codes.DeadlineExceeded, "timeout")
	}, 10*time.Millisecond, 2, time.Millisecond)

	allowed, err := client.CanJoinVoiceChannel(context.Background(), "ws", "ch", "user")
	require.NoError(t, err)
	require.False(t, allowed)
	require.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

func TestPermissionClient_CanJoinVoiceChannel_ContextCancelled(t *testing.T) {
	t.Parallel()

	client := newTestPermissionClient(t, func(ctx context.Context, _ *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, 50*time.Millisecond, 0, time.Millisecond)

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	allowed, err := client.CanJoinVoiceChannel(cancelledCtx, "ws", "ch", "user")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, allowed)
}

func newTestPermissionClient(
	t *testing.T,
	handler func(context.Context, *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error),
	timeout time.Duration,
	maxRetries int,
	retryBackoff time.Duration,
) *PermissionClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	permissionv1.RegisterPermissionServiceServer(server, &testPermissionService{handler: handler})

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}

	client, err := NewPermissionClient(Options{
		Address:      "bufnet",
		Timeout:      timeout,
		MaxRetries:   maxRetries,
		RetryBackoff: retryBackoff,
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return client
}

type testPermissionService struct {
	permissionv1.UnimplementedPermissionServiceServer

	handler func(context.Context, *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error)
}

func (s *testPermissionService) CanJoinVoiceChannel(ctx context.Context, request *permissionv1.CanJoinVoiceChannelRequest) (*permissionv1.CanJoinVoiceChannelResponse, error) {
	if s.handler == nil {
		return &permissionv1.CanJoinVoiceChannelResponse{Allowed: true}, nil
	}

	return s.handler(ctx, request)
}
