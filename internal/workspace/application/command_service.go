package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/c4erries/mint/internal/workspace/domain"
)

// CommandService handles workspace write commands from Kafka.
type CommandService struct {
	writeRepo   WriteRepository
	idGenerator func() string
	now         func() time.Time
}

type CommandServiceOptions struct {
	IDGenerator func() string
	Now         func() time.Time
}

func NewCommandService(writeRepo WriteRepository, options CommandServiceOptions) (*CommandService, error) {
	if writeRepo == nil {
		return nil, fmt.Errorf("workspace write repository is required")
	}

	if options.IDGenerator == nil || options.Now == nil {
		return nil, fmt.Errorf("workspace command service options are incomplete")
	}

	return &CommandService{writeRepo: writeRepo, idGenerator: options.IDGenerator, now: options.Now}, nil
}

func (s *CommandService) CreateWorkspace(ctx context.Context, command CreateWorkspaceCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	return s.writeRepo.WithTx(ctx, func(tx WriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return err
		}

		if processed {
			return nil
		}

		now := s.now().UTC()

		workspace, err := domain.NewWorkspace(command.Meta.WorkspaceID, command.WorkspaceName, command.Meta.ActorID, now)
		if err != nil {
			return mapDomainWriteError(err)
		}

		if err = tx.SaveWorkspace(ctx, workspace); err != nil {
			return err
		}

		owner, err := domain.NewMember(workspace.ID, command.Meta.ActorID, now)
		if err != nil {
			return mapDomainWriteError(err)
		}

		if err = tx.SaveMember(ctx, owner); err != nil {
			return err
		}

		if err = tx.AppendOutbox(ctx, buildOutboxMessage(command.Meta, domain.EventWorkspaceCreated, map[string]string{
			"workspace_name": workspace.Name,
		})); err != nil {
			return err
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return err
		}

		return nil
	})
}

func (s *CommandService) CreateChannel(ctx context.Context, command CreateChannelCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	return s.writeRepo.WithTx(ctx, func(tx WriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return err
		}

		if processed {
			return nil
		}

		if _, err = tx.GetWorkspace(ctx, command.Meta.WorkspaceID); err != nil {
			return mapDomainWriteError(err)
		}

		member, err := tx.GetMember(ctx, command.Meta.WorkspaceID, command.Meta.ActorID)
		if err != nil {
			return mapDomainWriteError(err)
		}

		if member.Banned {
			return domain.ErrPermissionDenied
		}

		channel, err := domain.NewChannel(command.Meta.WorkspaceID, command.Meta.ChannelID, command.ChannelName, command.ChannelKind, s.now().UTC())
		if err != nil {
			return mapDomainWriteError(err)
		}

		if err = tx.SaveChannel(ctx, channel); err != nil {
			return err
		}

		if err = tx.AppendOutbox(ctx, buildOutboxMessage(command.Meta, domain.EventChannelCreated, map[string]string{
			"channel_name": channel.Name,
			"channel_kind": channel.Kind,
		})); err != nil {
			return err
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return err
		}

		return nil
	})
}

func (s *CommandService) JoinWorkspace(ctx context.Context, command JoinWorkspaceCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	return s.writeRepo.WithTx(ctx, func(tx WriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return err
		}

		if processed {
			return nil
		}

		if _, err = tx.GetWorkspace(ctx, command.Meta.WorkspaceID); err != nil {
			return mapDomainWriteError(err)
		}

		member, err := tx.GetMember(ctx, command.Meta.WorkspaceID, command.UserID)
		switch {
		case err == nil:
			if member.Banned {
				return domain.ErrMemberBanned
			}
		case errors.Is(err, domain.ErrMemberNotFound):
			member, err = domain.NewMember(command.Meta.WorkspaceID, command.UserID, s.now().UTC())
			if err != nil {
				return mapDomainWriteError(err)
			}
		default:
			return err
		}

		if err = tx.SaveMember(ctx, member); err != nil {
			return err
		}

		if err = tx.AppendOutbox(ctx, buildOutboxMessage(command.Meta, domain.EventMemberJoined, map[string]string{
			"joined_user_id": command.UserID,
		})); err != nil {
			return err
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return err
		}

		return nil
	})
}

func (s *CommandService) BanMember(ctx context.Context, command BanMemberCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	return s.writeRepo.WithTx(ctx, func(tx WriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return err
		}

		if processed {
			return nil
		}

		if _, err = tx.GetWorkspace(ctx, command.Meta.WorkspaceID); err != nil {
			return mapDomainWriteError(err)
		}

		member, err := tx.GetMember(ctx, command.Meta.WorkspaceID, command.UserID)
		if err != nil {
			return mapDomainWriteError(err)
		}

		member.Ban()

		if err = tx.SaveMember(ctx, member); err != nil {
			return err
		}

		if err = tx.AppendOutbox(ctx, buildOutboxMessage(command.Meta, domain.EventMemberBanned, map[string]string{
			"banned_user_id": command.UserID,
		})); err != nil {
			return err
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return err
		}

		return nil
	})
}

func buildOutboxMessage(meta CommandMeta, eventType string, payload map[string]string) OutboxMessage {
	return OutboxMessage{
		EventID:       meta.CommandID + ":" + eventType,
		EventType:     eventType,
		CommandID:     meta.CommandID,
		CorrelationID: meta.CorrelationID,
		CausationID:   meta.CausationID,
		MessageID:     meta.MessageID,
		OccurredAt:    meta.OccurredAt,
		WorkspaceID:   meta.WorkspaceID,
		ChannelID:     meta.ChannelID,
		ActorID:       meta.ActorID,
		SchemaVersion: meta.SchemaVersion,
		Payload:       payload,
	}
}

func mapDomainWriteError(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidIdentifier), errors.Is(err, domain.ErrInvalidName), errors.Is(err, domain.ErrInvalidTimestamp):
		return ErrInvalidCommand
	default:
		return err
	}
}
