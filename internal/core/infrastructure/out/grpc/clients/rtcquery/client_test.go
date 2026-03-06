package rtcquery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rtcv1 "github.com/c4erries/mint/api/rtc/v1"
)

type fakeRTCClient struct {
	getVoiceRoomStateFunc func(ctx context.Context, request *rtcv1.GetVoiceRoomStateRequest, opts ...grpc.CallOption) (*rtcv1.GetVoiceRoomStateResponse, error)
}

func (c *fakeRTCClient) GetVoiceRoomState(ctx context.Context, request *rtcv1.GetVoiceRoomStateRequest, opts ...grpc.CallOption) (*rtcv1.GetVoiceRoomStateResponse, error) {
	if c.getVoiceRoomStateFunc == nil {
		return &rtcv1.GetVoiceRoomStateResponse{}, nil
	}

	return c.getVoiceRoomStateFunc(ctx, request, opts...)
}

func (c *fakeRTCClient) ListVoiceParticipants(_ context.Context, _ *rtcv1.ListVoiceParticipantsRequest, _ ...grpc.CallOption) (*rtcv1.ListVoiceParticipantsResponse, error) {
	return &rtcv1.ListVoiceParticipantsResponse{}, nil
}

func (c *fakeRTCClient) GetVoiceChannelBinding(_ context.Context, _ *rtcv1.GetVoiceChannelBindingRequest, _ ...grpc.CallOption) (*rtcv1.GetVoiceChannelBindingResponse, error) {
	return &rtcv1.GetVoiceChannelBindingResponse{}, nil
}

func (c *fakeRTCClient) GetRtcTokenGrantStatus(_ context.Context, _ *rtcv1.GetRtcTokenGrantStatusRequest, _ ...grpc.CallOption) (*rtcv1.GetRtcTokenGrantStatusResponse, error) {
	return &rtcv1.GetRtcTokenGrantStatusResponse{}, nil
}

type fakeCloser struct {
	closed bool
}

func (c *fakeCloser) Close() error {
	c.closed = true

	return nil
}

func TestNewClient_Validation(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Options{MaxRetries: -1, Client: &fakeRTCClient{}})
	require.Error(t, err)
	require.ErrorContains(t, err, "rtc max retries must be >= 0")

	_, err = NewClient(Options{})
	require.Error(t, err)
	require.ErrorContains(t, err, "rtc grpc address is required")
}

func TestGetVoiceRoomState_RetryOnUnavailable(t *testing.T) {
	t.Parallel()

	attempts := 0
	client, err := NewClient(Options{
		Client: &fakeRTCClient{
			getVoiceRoomStateFunc: func(_ context.Context, _ *rtcv1.GetVoiceRoomStateRequest, _ ...grpc.CallOption) (*rtcv1.GetVoiceRoomStateResponse, error) {
				attempts++
				if attempts == 1 {
					return nil, status.Error(codes.Unavailable, "temporary")
				}

				return &rtcv1.GetVoiceRoomStateResponse{}, nil
			},
		},
		Timeout:      time.Second,
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
	})
	require.NoError(t, err)

	_, err = client.GetVoiceRoomState(context.Background(), "ws", "ch")
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

func TestGetVoiceRoomState_NoRetryOnInvalidArgument(t *testing.T) {
	t.Parallel()

	attempts := 0
	client, err := NewClient(Options{
		Client: &fakeRTCClient{
			getVoiceRoomStateFunc: func(_ context.Context, _ *rtcv1.GetVoiceRoomStateRequest, _ ...grpc.CallOption) (*rtcv1.GetVoiceRoomStateResponse, error) {
				attempts++
				return nil, status.Error(codes.InvalidArgument, "invalid")
			},
		},
		Timeout:      time.Second,
		MaxRetries:   2,
		RetryBackoff: time.Millisecond,
	})
	require.NoError(t, err)

	_, err = client.GetVoiceRoomState(context.Background(), "ws", "ch")
	require.Error(t, err)
	require.Equal(t, 1, attempts)
}

func TestGetVoiceRoomState_ReturnsContextError(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Options{
		Client: &fakeRTCClient{
			getVoiceRoomStateFunc: func(_ context.Context, _ *rtcv1.GetVoiceRoomStateRequest, _ ...grpc.CallOption) (*rtcv1.GetVoiceRoomStateResponse, error) {
				return nil, status.Error(codes.Unavailable, "retry")
			},
		},
		Timeout:      time.Second,
		MaxRetries:   3,
		RetryBackoff: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.GetVoiceRoomState(ctx, "ws", "ch")
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

func TestClose_UsesProvidedCloser(t *testing.T) {
	t.Parallel()

	closer := &fakeCloser{}
	client, err := NewClient(Options{
		Client: &fakeRTCClient{},
		Closer: closer,
	})
	require.NoError(t, err)

	err = client.Close()
	require.NoError(t, err)
	require.True(t, closer.closed)

	nilClient := (*Client)(nil)
	require.NoError(t, nilClient.Close())

	client.closer = nil
	require.NoError(t, client.Close())
}
