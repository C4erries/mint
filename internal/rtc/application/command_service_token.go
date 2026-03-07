package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/c4erries/mint/internal/rtc/domain"
)

func (s *CommandService) IssueRtcToken(ctx context.Context, command IssueRtcTokenCommand) error {
	if err := command.Validate(); err != nil {
		return err
	}

	if err := s.ensureJoinPermission(ctx, command.Meta.WorkspaceID, command.Meta.ChannelID, command.UserID); err != nil {
		return err
	}

	ttl := s.resolveTokenTTL(command.TTL)

	return s.rooms.WithTx(ctx, func(tx VoiceRoomWriteTx) error {
		processed, err := tx.IsCommandProcessed(ctx, command.Meta.CommandID)
		if err != nil {
			return fmt.Errorf("check processed issue token command: %w", err)
		}

		if processed {
			return nil
		}

		room, err := s.loadRoom(ctx, tx, command.Meta)
		if err != nil {
			return err
		}

		if err = ensureParticipantActive(room, command.UserID); err != nil {
			return err
		}

		savedGrant, err := s.loadOrIssueGrant(ctx, room, command, ttl)
		if err != nil {
			return err
		}

		return s.persistIssuedToken(ctx, tx, room, command, ttl, savedGrant)
	})
}

func (s *CommandService) resolveTokenTTL(ttl time.Duration) time.Duration {
	if ttl > 0 {
		return ttl
	}

	return s.defaultTokenTTL
}

func ensureParticipantActive(room *domain.VoiceRoom, userID string) error {
	participant, found := room.Participant(userID)
	if !found || participant.LeftAt != nil {
		return domain.ErrParticipantNotFound
	}

	return nil
}

func (s *CommandService) loadOrIssueGrant(
	ctx context.Context,
	room *domain.VoiceRoom,
	command IssueRtcTokenCommand,
	ttl time.Duration,
) (domain.MediaAccessGrant, error) {
	grant, err := s.grants.GetGrantByCommandID(ctx, command.Meta.CommandID)
	if err == nil {
		return grant, nil
	}

	if !errors.Is(err, domain.ErrGrantNotFound) && !errors.Is(err, domain.ErrGrantExpired) {
		return domain.MediaAccessGrant{}, fmt.Errorf("load grant by command id: %w", err)
	}

	return s.issueAndSaveGrant(ctx, room, command, ttl)
}

func (s *CommandService) issueAndSaveGrant(
	ctx context.Context,
	room *domain.VoiceRoom,
	command IssueRtcTokenCommand,
	ttl time.Duration,
) (domain.MediaAccessGrant, error) {
	issuedToken, err := s.livekit.IssueToken(ctx, LiveKitTokenRequest{
		RoomID:       room.ID,
		UserID:       command.UserID,
		TTL:          ttl,
		CanPublish:   command.CanPublish,
		CanSubscribe: command.CanSubscribe,
	})
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("issue livekit token: %w", err)
	}

	issuedAt := issuedToken.IssuedAt
	if issuedAt.IsZero() {
		issuedAt = s.now()
	}

	expiresAt := issuedToken.ExpiresAt
	if expiresAt.IsZero() {
		expiresAt = issuedAt.Add(ttl)
	}

	grant, err := domain.NewMediaAccessGrant(
		command.Meta.CommandID,
		command.Meta.CommandID,
		room.ID,
		command.UserID,
		issuedToken.Token,
		command.CanPublish,
		command.CanSubscribe,
		issuedAt,
		expiresAt,
	)
	if err != nil {
		return domain.MediaAccessGrant{}, fmt.Errorf("create grant: %w", err)
	}

	if err = s.grants.SaveGrant(ctx, grant); err != nil {
		if !errors.Is(err, domain.ErrGrantAlreadyExists) {
			return domain.MediaAccessGrant{}, fmt.Errorf("save grant: %w", err)
		}

		existingGrant, lookupErr := s.grants.GetGrantByCommandID(ctx, command.Meta.CommandID)
		if lookupErr != nil {
			return domain.MediaAccessGrant{}, fmt.Errorf("load existing grant by command id: %w", lookupErr)
		}

		return existingGrant, nil
	}

	return grant, nil
}

func (s *CommandService) persistIssuedToken(
	ctx context.Context,
	tx VoiceRoomWriteTx,
	room *domain.VoiceRoom,
	command IssueRtcTokenCommand,
	ttl time.Duration,
	grant domain.MediaAccessGrant,
) error {
	if err := room.Touch(command.Meta.OccurredAt); err != nil {
		return fmt.Errorf("touch room state: %w", err)
	}

	if err := tx.SaveRoom(ctx, room); err != nil {
		return fmt.Errorf("save room after token issue: %w", err)
	}

	message := s.newOutboxMessage(command.Meta, domain.EventRtcTokenIssued, room.ID, map[string]string{
		"token_id":    grant.TokenID,
		"user_id":     command.UserID,
		"expires_at":  grant.ExpiresAt.Format(time.RFC3339Nano),
		"ttl_seconds": strconv.FormatInt(int64(ttl.Seconds()), 10),
	})
	if err := tx.AppendOutbox(ctx, message); err != nil {
		return fmt.Errorf("append outbox token issue: %w", err)
	}

	if err := tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
		return fmt.Errorf("mark issue token command processed: %w", err)
	}

	return nil
}
