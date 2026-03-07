package scyllarepo

import (
	"context"

	"github.com/c4erries/mint/internal/rtc/application"
	"github.com/c4erries/mint/internal/rtc/domain"
)

type transaction struct {
	store *Store

	loadedRooms    map[string]*domain.VoiceRoom
	dirtyRooms     map[string]struct{}
	loadedBindings map[string]*domain.VoiceChannelBinding
	dirtyBindings  map[string]struct{}

	outbox         []application.OutboxMessage
	commandsToMark map[string]struct{}
}

func newTransaction(store *Store) *transaction {
	return &transaction{
		store:          store,
		loadedRooms:    make(map[string]*domain.VoiceRoom),
		dirtyRooms:     make(map[string]struct{}),
		loadedBindings: make(map[string]*domain.VoiceChannelBinding),
		dirtyBindings:  make(map[string]struct{}),
		outbox:         make([]application.OutboxMessage, 0),
		commandsToMark: make(map[string]struct{}),
	}
}

func (t *transaction) GetRoom(ctx context.Context, roomID string) (*domain.VoiceRoom, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if room, ok := t.loadedRooms[roomID]; ok {
		return room.Clone(), nil
	}

	room, err := t.store.getRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}

	t.loadedRooms[roomID] = room.Clone()

	return room.Clone(), nil
}

func (t *transaction) SaveRoom(ctx context.Context, room *domain.VoiceRoom) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if room == nil {
		return domain.ErrInvalidIdentifier
	}

	t.loadedRooms[room.ID] = room.Clone()
	t.dirtyRooms[room.ID] = struct{}{}

	return nil
}

func (t *transaction) GetBindingByChannel(ctx context.Context, workspaceID string, channelID string) (*domain.VoiceChannelBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	key := bindingKey(workspaceID, channelID)
	if binding, ok := t.loadedBindings[key]; ok {
		return binding.Clone(), nil
	}

	binding, err := t.store.getBinding(ctx, workspaceID, channelID)
	if err != nil {
		return nil, err
	}

	t.loadedBindings[key] = binding.Clone()

	return binding.Clone(), nil
}

func (t *transaction) SaveBinding(ctx context.Context, binding *domain.VoiceChannelBinding) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if binding == nil {
		return domain.ErrInvalidIdentifier
	}

	key := bindingKey(binding.WorkspaceID, binding.ChannelID)
	t.loadedBindings[key] = binding.Clone()
	t.dirtyBindings[key] = struct{}{}

	return nil
}

func (t *transaction) AppendOutbox(ctx context.Context, message application.OutboxMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if message.EventID == "" || message.EventType == "" {
		return domain.ErrInvalidIdentifier
	}

	copied := message
	if message.Payload != nil {
		copied.Payload = make(map[string]string, len(message.Payload))
		for key, value := range message.Payload {
			copied.Payload[key] = value
		}
	}

	t.outbox = append(t.outbox, copied)

	return nil
}

func (t *transaction) IsCommandProcessed(ctx context.Context, commandID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	if _, marked := t.commandsToMark[commandID]; marked {
		return true, nil
	}

	return t.store.isCommandProcessed(ctx, commandID)
}

func (t *transaction) MarkCommandProcessed(ctx context.Context, commandID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if commandID == "" {
		return domain.ErrInvalidIdentifier
	}

	t.commandsToMark[commandID] = struct{}{}

	return nil
}
