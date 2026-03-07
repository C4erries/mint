package domain

import (
	"errors"
	"testing"
	"time"
)

func TestVoiceRoomJoinParticipant(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

	testCases := []struct {
		name        string
		preJoin     bool
		roomActive  bool
		expectedErr error
	}{
		{
			name:        "success",
			preJoin:     false,
			roomActive:  true,
			expectedErr: nil,
		},
		{
			name:        "already joined",
			preJoin:     true,
			roomActive:  true,
			expectedErr: ErrParticipantAlreadyJoined,
		},
		{
			name:        "inactive room",
			preJoin:     false,
			roomActive:  false,
			expectedErr: ErrRoomInactive,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			room, err := NewVoiceRoom("room-1", "ws-1", "ch-1", now)
			if err != nil {
				t.Fatalf("unexpected error creating room: %v", err)
			}

			if testCase.preJoin {
				_, err = room.JoinParticipant("user-1", now)
				if err != nil {
					t.Fatalf("unexpected pre-join error: %v", err)
				}
			}

			if !testCase.roomActive {
				room.Active = false
			}

			_, err = room.JoinParticipant("user-1", now.Add(time.Second))
			if testCase.expectedErr == nil && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if testCase.expectedErr != nil && !errors.Is(err, testCase.expectedErr) {
				t.Fatalf("expected error %v, got %v", testCase.expectedErr, err)
			}
		})
	}
}

func TestVoiceRoomParticipantStateChanges(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

	room, err := NewVoiceRoom("room-2", "ws-1", "ch-1", now)
	if err != nil {
		t.Fatalf("unexpected error creating room: %v", err)
	}

	if _, err = room.JoinParticipant("user-2", now); err != nil {
		t.Fatalf("unexpected error joining room: %v", err)
	}

	if _, err = room.SetMicrophoneMuted("user-2", true, now.Add(1*time.Second)); err != nil {
		t.Fatalf("unexpected error muting participant: %v", err)
	}

	if _, err = room.SetCameraEnabled("user-2", true, now.Add(2*time.Second)); err != nil {
		t.Fatalf("unexpected error enabling camera: %v", err)
	}

	participant, found := room.Participant("user-2")
	if !found {
		t.Fatalf("participant should exist")
	}

	if !participant.MicrophoneMuted {
		t.Fatalf("participant should be muted")
	}

	if !participant.CameraEnabled {
		t.Fatalf("participant camera should be enabled")
	}

	if _, err = room.LeaveParticipant("user-2", now.Add(3*time.Second)); err != nil {
		t.Fatalf("unexpected error leaving room: %v", err)
	}

	if room.ActiveParticipantCount() != 0 {
		t.Fatalf("expected no active participants")
	}

	if room.Active {
		t.Fatalf("room should become inactive after last participant left")
	}
}
