package inmemory

import (
	"context"
	"fmt"
	"sync"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

type outboxRecord struct {
	Message   application.OutboxMessage
	Published bool
}

// RoomStore is an in-memory implementation that mimics transactional write-model + outbox behavior.
type RoomStore struct {
	mu sync.RWMutex

	rooms             map[string]*domain.VoiceRoom
	bindings          map[string]*domain.VoiceChannelBinding
	processedCommands map[string]struct{}
	outbox            []outboxRecord
}

func NewRoomStore() *RoomStore {
	return &RoomStore{
		rooms:             make(map[string]*domain.VoiceRoom),
		bindings:          make(map[string]*domain.VoiceChannelBinding),
		processedCommands: make(map[string]struct{}),
		outbox:            make([]outboxRecord, 0),
	}
}

func (s *RoomStore) WithTx(ctx context.Context, fn func(tx application.VoiceRoomWriteTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx := &transaction{
		rooms:             cloneRooms(s.rooms),
		bindings:          cloneBindings(s.bindings),
		processedCommands: cloneProcessedCommands(s.processedCommands),
		outbox:            cloneOutbox(s.outbox),
	}

	if err := fn(tx); err != nil {
		return err
	}

	s.rooms = tx.rooms
	s.bindings = tx.bindings
	s.processedCommands = tx.processedCommands
	s.outbox = tx.outbox

	return nil
}

func (s *RoomStore) GetVoiceRoomState(ctx context.Context, workspaceID string, channelID string) (application.VoiceRoomState, error) {
	if err := ctx.Err(); err != nil {
		return application.VoiceRoomState{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	binding, ok := s.bindings[bindingKey(workspaceID, channelID)]
	if !ok {
		return application.VoiceRoomState{}, domain.ErrVoiceChannelBindingNotFound
	}

	room, ok := s.rooms[binding.RoomID]
	if !ok {
		return application.VoiceRoomState{}, domain.ErrVoiceRoomNotFound
	}

	return application.VoiceRoomState{
		RoomID:            room.ID,
		WorkspaceID:       room.WorkspaceID,
		ChannelID:         room.ChannelID,
		Active:            room.Active,
		ParticipantCount:  room.ActiveParticipantCount(),
		LastStateChangeAt: room.UpdatedAt,
	}, nil
}

func (s *RoomStore) ListVoiceParticipants(ctx context.Context, roomID string) ([]application.VoiceParticipant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	room, ok := s.rooms[roomID]
	if !ok {
		return nil, domain.ErrVoiceRoomNotFound
	}

	participants := room.Participants()
	result := make([]application.VoiceParticipant, 0, len(participants))

	for _, participant := range participants {
		result = append(result, application.VoiceParticipant{
			UserID:          participant.UserID,
			JoinedAt:        participant.JoinedAt,
			LeftAt:          participant.LeftAt,
			MicrophoneMuted: participant.MicrophoneMuted,
			CameraEnabled:   participant.CameraEnabled,
		})
	}

	return result, nil
}

func (s *RoomStore) GetVoiceChannelBinding(ctx context.Context, workspaceID string, channelID string) (application.VoiceChannelBindingView, error) {
	if err := ctx.Err(); err != nil {
		return application.VoiceChannelBindingView{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	binding, ok := s.bindings[bindingKey(workspaceID, channelID)]
	if !ok {
		return application.VoiceChannelBindingView{}, domain.ErrVoiceChannelBindingNotFound
	}

	return application.VoiceChannelBindingView{
		WorkspaceID: binding.WorkspaceID,
		ChannelID:   binding.ChannelID,
		RoomID:      binding.RoomID,
		UpdatedAt:   binding.UpdatedAt,
	}, nil
}

func (s *RoomStore) ListUnpublished(ctx context.Context, limit int) ([]application.OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		limit = len(s.outbox)
	}

	result := make([]application.OutboxMessage, 0, limit)

	for _, record := range s.outbox {
		if record.Published {
			continue
		}

		result = append(result, cloneMessage(record.Message))
		if len(result) >= limit {
			break
		}
	}

	return result, nil
}

func (s *RoomStore) MarkPublished(ctx context.Context, eventID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for index := range s.outbox {
		if s.outbox[index].Message.EventID != eventID {
			continue
		}

		s.outbox[index].Published = true

		return nil
	}

	return fmt.Errorf("mark event %s as published: %w", eventID, domain.ErrInvalidIdentifier)
}

type transaction struct {
	rooms             map[string]*domain.VoiceRoom
	bindings          map[string]*domain.VoiceChannelBinding
	processedCommands map[string]struct{}
	outbox            []outboxRecord
}

func (t *transaction) GetRoom(ctx context.Context, roomID string) (*domain.VoiceRoom, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	room, ok := t.rooms[roomID]
	if !ok {
		return nil, domain.ErrVoiceRoomNotFound
	}

	return room.Clone(), nil
}

func (t *transaction) SaveRoom(ctx context.Context, room *domain.VoiceRoom) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if room == nil {
		return domain.ErrInvalidIdentifier
	}

	t.rooms[room.ID] = room.Clone()

	return nil
}

func (t *transaction) GetBindingByChannel(ctx context.Context, workspaceID string, channelID string) (*domain.VoiceChannelBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	binding, ok := t.bindings[bindingKey(workspaceID, channelID)]
	if !ok {
		return nil, domain.ErrVoiceChannelBindingNotFound
	}

	return binding.Clone(), nil
}

func (t *transaction) SaveBinding(ctx context.Context, binding *domain.VoiceChannelBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if binding == nil {
		return domain.ErrInvalidIdentifier
	}

	t.bindings[bindingKey(binding.WorkspaceID, binding.ChannelID)] = binding.Clone()

	return nil
}

func (t *transaction) AppendOutbox(ctx context.Context, message application.OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if message.EventID == "" || message.EventType == "" {
		return domain.ErrInvalidIdentifier
	}

	t.outbox = append(t.outbox, outboxRecord{
		Message:   cloneMessage(message),
		Published: false,
	})

	return nil
}

func (t *transaction) IsCommandProcessed(ctx context.Context, commandID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	_, processed := t.processedCommands[commandID]

	return processed, nil
}

func (t *transaction) MarkCommandProcessed(ctx context.Context, commandID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if commandID == "" {
		return domain.ErrInvalidIdentifier
	}

	t.processedCommands[commandID] = struct{}{}

	return nil
}

func bindingKey(workspaceID string, channelID string) string {
	return workspaceID + ":" + channelID
}

func cloneRooms(source map[string]*domain.VoiceRoom) map[string]*domain.VoiceRoom {
	copied := make(map[string]*domain.VoiceRoom, len(source))

	for key, room := range source {
		copied[key] = room.Clone()
	}

	return copied
}

func cloneBindings(source map[string]*domain.VoiceChannelBinding) map[string]*domain.VoiceChannelBinding {
	copied := make(map[string]*domain.VoiceChannelBinding, len(source))

	for key, binding := range source {
		copied[key] = binding.Clone()
	}

	return copied
}

func cloneProcessedCommands(source map[string]struct{}) map[string]struct{} {
	copied := make(map[string]struct{}, len(source))

	for key := range source {
		copied[key] = struct{}{}
	}

	return copied
}

func cloneOutbox(source []outboxRecord) []outboxRecord {
	copied := make([]outboxRecord, 0, len(source))

	for _, record := range source {
		copied = append(copied, outboxRecord{
			Message:   cloneMessage(record.Message),
			Published: record.Published,
		})
	}

	return copied
}

func cloneMessage(message application.OutboxMessage) application.OutboxMessage {
	copied := message
	if message.Payload != nil {
		copied.Payload = make(map[string]string, len(message.Payload))
		for key, value := range message.Payload {
			copied.Payload[key] = value
		}
	}

	return copied
}
