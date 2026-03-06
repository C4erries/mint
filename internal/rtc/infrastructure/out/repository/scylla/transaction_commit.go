package scyllarepo

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

func (t *transaction) commit(ctx context.Context) error {
	if err := t.commitWriteBatch(ctx); err != nil {
		return err
	}

	commandIDs := sortedKeys(t.commandsToMark)
	for _, commandID := range commandIDs {
		if err := t.store.markCommandProcessed(ctx, commandID); err != nil {
			return err
		}
	}

	return nil
}

func (t *transaction) commitWriteBatch(ctx context.Context) error {
	batch := t.store.rawSession.Batch(gocql.LoggedBatch).WithContext(ctx)

	entries := 0

	added, err := t.appendRoomQueries(batch)
	if err != nil {
		return err
	}
	entries += added

	added, err = t.appendBindingQueries(batch)
	if err != nil {
		return err
	}
	entries += added

	added, err = t.appendOutboxQueries(batch)
	if err != nil {
		return err
	}
	entries += added

	if entries == 0 {
		return nil
	}

	if err = t.store.rawSession.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("execute scylla write batch: %w", err)
	}

	return nil
}

func (t *transaction) appendRoomQueries(batch *gocql.Batch) (int, error) {
	entries := 0

	roomIDs := sortedKeys(t.dirtyRooms)
	for _, roomID := range roomIDs {
		room, ok := t.loadedRooms[roomID]
		if !ok {
			continue
		}

		row, err := toRoomRow(room)
		if err != nil {
			return 0, err
		}

		batch.Query(
			insertRoomCQL,
			row.RoomID,
			row.WorkspaceID,
			row.ChannelID,
			row.Active,
			row.CreatedAt,
			row.UpdatedAt,
			row.ParticipantsJSON,
		)

		entries++
	}

	return entries, nil
}

func (t *transaction) appendBindingQueries(batch *gocql.Batch) (int, error) {
	entries := 0

	bindingIDs := sortedKeys(t.dirtyBindings)
	for _, key := range bindingIDs {
		binding, ok := t.loadedBindings[key]
		if !ok {
			continue
		}

		row := toBindingRow(binding)
		batch.Query(
			insertBindingCQL,
			row.WorkspaceID,
			row.ChannelID,
			row.RoomID,
			row.BoundAt,
			row.UpdatedAt,
		)

		entries++
	}

	return entries, nil
}

func (t *transaction) appendOutboxQueries(batch *gocql.Batch) (int, error) {
	entries := 0

	for i := range t.outbox {
		row, err := toOutboxRow(t.outbox[i])
		if err != nil {
			return 0, err
		}

		batch.Query(
			insertOutboxCQL,
			row.EventID,
			row.EventType,
			row.CommandID,
			row.CorrelationID,
			row.CausationID,
			row.MessageID,
			row.OccurredAt,
			row.WorkspaceID,
			row.ChannelID,
			row.RoomID,
			row.ActorID,
			row.SchemaVersion,
			row.PayloadJSON,
			row.Published,
		)

		entries++
	}

	return entries, nil
}
