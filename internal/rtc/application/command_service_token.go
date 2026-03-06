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

	ttl := command.TTL
	if ttl == 0 {
		ttl = s.defaultTokenTTL
	}

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

		participant, found := room.Participant(command.UserID)
		if !found || participant.LeftAt != nil {
			return domain.ErrParticipantNotFound
		}

		savedGrant, err := s.grants.GetGrantByCommandID(ctx, command.Meta.CommandID)
		if err != nil {
			if !errors.Is(err, domain.ErrGrantNotFound) && !errors.Is(err, domain.ErrGrantExpired) {
				return fmt.Errorf("load grant by command id: %w", err)
			}

			issuedToken, issueErr := s.livekit.IssueToken(ctx, LiveKitTokenRequest{
				RoomID:       room.ID,
				UserID:       command.UserID,
				TTL:          ttl,
				CanPublish:   command.CanPublish,
				CanSubscribe: command.CanSubscribe,
			})
			if issueErr != nil {
				return fmt.Errorf("issue livekit token: %w", issueErr)
			}

			issuedAt := issuedToken.IssuedAt
			if issuedAt.IsZero() {
				issuedAt = s.now()
			}

			expiresAt := issuedToken.ExpiresAt
			if expiresAt.IsZero() {
				expiresAt = issuedAt.Add(ttl)
			}

			grant, createErr := domain.NewMediaAccessGrant(
				issuedToken.TokenID,
				command.Meta.CommandID,
				room.ID,
				command.UserID,
				issuedToken.Token,
				command.CanPublish,
				command.CanSubscribe,
				issuedAt,
				expiresAt,
			)
			if createErr != nil {
				return fmt.Errorf("create grant: %w", createErr)
			}

			savedGrant = grant
			if saveErr := s.grants.SaveGrant(ctx, grant); saveErr != nil {
				if !errors.Is(saveErr, domain.ErrGrantAlreadyExists) {
					return fmt.Errorf("save grant: %w", saveErr)
				}

				savedGrant, saveErr = s.grants.GetGrantByCommandID(ctx, command.Meta.CommandID)
				if saveErr != nil {
					return fmt.Errorf("load existing grant by command id: %w", saveErr)
				}
			}
		}

		if err = room.Touch(command.Meta.OccurredAt); err != nil {
			return fmt.Errorf("touch room state: %w", err)
		}

		if err = tx.SaveRoom(ctx, room); err != nil {
			return fmt.Errorf("save room after token issue: %w", err)
		}

		message := s.newOutboxMessage(command.Meta, domain.EventRtcTokenIssued, room.ID, map[string]string{
			"token_id":    savedGrant.TokenID,
			"user_id":     command.UserID,
			"expires_at":  savedGrant.ExpiresAt.Format(time.RFC3339Nano),
			"ttl_seconds": strconv.FormatInt(int64(ttl.Seconds()), 10),
		})
		if err = tx.AppendOutbox(ctx, message); err != nil {
			return fmt.Errorf("append outbox token issue: %w", err)
		}

		if err = tx.MarkCommandProcessed(ctx, command.Meta.CommandID); err != nil {
			return fmt.Errorf("mark issue token command processed: %w", err)
		}

		return nil
	})
}
