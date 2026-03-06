package application

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/c4erries/mint/internal/identity/domain"
)

// Service implements identity auth use cases.
type Service struct {
	accounts        AccountRepository
	sessions        SessionRepository
	revocations     RevocationStore
	hasher          PasswordHasher
	tokens          TokenManager
	idGenerator     func() string
	now             func() time.Time
	accessTokenTTL  time.Duration
	refreshTokenTTL time.Duration
}

type ServiceOptions struct {
	IDGenerator     func() string
	Now             func() time.Time
	AccessTokenTTL  time.Duration
	RefreshTokenTTL time.Duration
}

func NewService(
	accounts AccountRepository,
	sessions SessionRepository,
	revocations RevocationStore,
	hasher PasswordHasher,
	tokens TokenManager,
	options ServiceOptions,
) (*Service, error) {
	if accounts == nil || sessions == nil || revocations == nil || hasher == nil || tokens == nil {
		return nil, fmt.Errorf("identity service dependencies are required")
	}

	if options.IDGenerator == nil || options.Now == nil {
		return nil, fmt.Errorf("identity service options are incomplete")
	}

	if options.AccessTokenTTL <= 0 || options.RefreshTokenTTL <= 0 {
		return nil, fmt.Errorf("identity token ttl must be positive")
	}

	return &Service{
		accounts:        accounts,
		sessions:        sessions,
		revocations:     revocations,
		hasher:          hasher,
		tokens:          tokens,
		idGenerator:     options.IDGenerator,
		now:             options.Now,
		accessTokenTTL:  options.AccessTokenTTL,
		refreshTokenTTL: options.RefreshTokenTTL,
	}, nil
}

func (s *Service) Register(ctx context.Context, command RegisterCommand) (AuthResult, error) {
	if err := validateAuthCommand(command.Email, command.Password); err != nil {
		return AuthResult{}, err
	}

	_, err := s.accounts.GetAccountByEmail(ctx, command.Email)
	switch {
	case err == nil:
		return AuthResult{}, ErrAccountAlreadyExist
	case !errors.Is(err, domain.ErrAccountNotFound):
		return AuthResult{}, err
	}

	passwordHash, err := s.hasher.Hash(command.Password)
	if err != nil {
		return AuthResult{}, fmt.Errorf("hash password: %w", err)
	}

	now := s.now().UTC()
	account, err := domain.NewAccount(s.idGenerator(), command.Email, passwordHash, now)
	if err != nil {
		return AuthResult{}, mapDomainValidationError(err)
	}

	if err = s.accounts.CreateAccount(ctx, account); err != nil {
		return AuthResult{}, err
	}

	return s.issueSessionTokens(ctx, account, command.UserAgent, command.IP)
}

func (s *Service) Login(ctx context.Context, command LoginCommand) (AuthResult, error) {
	if err := validateAuthCommand(command.Email, command.Password); err != nil {
		return AuthResult{}, err
	}

	account, err := s.accounts.GetAccountByEmail(ctx, command.Email)
	if err != nil {
		if errors.Is(err, domain.ErrAccountNotFound) {
			return AuthResult{}, ErrInvalidCredentials
		}

		return AuthResult{}, err
	}

	if err = s.hasher.Compare(account.PasswordHash, command.Password); err != nil {
		return AuthResult{}, ErrInvalidCredentials
	}

	return s.issueSessionTokens(ctx, account, command.UserAgent, command.IP)
}

func (s *Service) Refresh(ctx context.Context, command RefreshCommand) (AuthResult, error) {
	if strings.TrimSpace(command.RefreshToken) == "" {
		return AuthResult{}, ErrInvalidCommand
	}

	refreshClaims, err := s.tokens.ParseToken(command.RefreshToken, TokenTypeRefresh)
	if err != nil {
		return AuthResult{}, mapTokenError(err)
	}

	revoked, err := s.revocations.IsRevoked(ctx, refreshClaims.TokenID)
	if err != nil {
		return AuthResult{}, err
	}

	if revoked {
		return AuthResult{}, ErrUnauthorized
	}

	session, err := s.sessions.GetSessionByRefreshJTI(ctx, refreshClaims.TokenID)
	if err != nil {
		if errors.Is(err, domain.ErrSessionNotFound) {
			return AuthResult{}, ErrUnauthorized
		}

		return AuthResult{}, err
	}

	now := s.now().UTC()
	if session.IsRevoked() || session.IsExpired(now) {
		return AuthResult{}, ErrUnauthorized
	}

	if session.AccountID != refreshClaims.AccountID || session.ID != refreshClaims.SessionID {
		return AuthResult{}, ErrUnauthorized
	}

	newRefreshToken, err := s.tokens.IssueToken(session.AccountID, session.ID, TokenTypeRefresh, s.refreshTokenTTL, now)
	if err != nil {
		return AuthResult{}, fmt.Errorf("issue refresh token: %w", err)
	}

	if err = s.sessions.UpdateSessionRefresh(ctx, session.ID, refreshClaims.TokenID, newRefreshToken.Claims.TokenID, newRefreshToken.Claims.ExpiresAt); err != nil {
		return AuthResult{}, err
	}

	if err = s.revocations.MarkRevoked(ctx, refreshClaims.TokenID, refreshClaims.ExpiresAt); err != nil {
		return AuthResult{}, err
	}

	accessToken, err := s.tokens.IssueToken(session.AccountID, session.ID, TokenTypeAccess, s.accessTokenTTL, now)
	if err != nil {
		return AuthResult{}, fmt.Errorf("issue access token: %w", err)
	}

	account, err := s.accounts.GetAccountByID(ctx, session.AccountID)
	if err != nil {
		return AuthResult{}, err
	}

	return AuthResult{
		AccountID:            account.ID,
		Email:                account.Email,
		SessionID:            session.ID,
		AccessToken:          accessToken.Raw,
		RefreshToken:         newRefreshToken.Raw,
		AccessTokenExpiresAt: accessToken.Claims.ExpiresAt,
		RefreshTokenExpireAt: newRefreshToken.Claims.ExpiresAt,
	}, nil
}

func (s *Service) Logout(ctx context.Context, command LogoutCommand) error {
	if command.AccessClaims.TokenID == "" || command.AccessClaims.SessionID == "" {
		return ErrInvalidCommand
	}

	now := s.now().UTC()

	if err := s.revocations.MarkRevoked(ctx, command.AccessClaims.TokenID, command.AccessClaims.ExpiresAt); err != nil {
		return err
	}

	if err := s.sessions.RevokeSession(ctx, command.AccessClaims.SessionID, now); err != nil {
		if !errors.Is(err, domain.ErrSessionNotFound) {
			return err
		}
	}

	if strings.TrimSpace(command.RefreshToken) != "" {
		refreshClaims, err := s.tokens.ParseToken(command.RefreshToken, TokenTypeRefresh)
		if err == nil {
			_ = s.revocations.MarkRevoked(ctx, refreshClaims.TokenID, refreshClaims.ExpiresAt)
		}
	}

	return nil
}

func (s *Service) Me(ctx context.Context, claims TokenClaims) (MeResult, error) {
	if claims.AccountID == "" || claims.TokenType != TokenTypeAccess {
		return MeResult{}, ErrUnauthorized
	}

	revoked, err := s.revocations.IsRevoked(ctx, claims.TokenID)
	if err != nil {
		return MeResult{}, err
	}

	if revoked {
		return MeResult{}, ErrUnauthorized
	}

	account, err := s.accounts.GetAccountByID(ctx, claims.AccountID)
	if err != nil {
		if errors.Is(err, domain.ErrAccountNotFound) {
			return MeResult{}, ErrUnauthorized
		}

		return MeResult{}, err
	}

	return MeResult{AccountID: account.ID, Email: account.Email}, nil
}

func (s *Service) ParseAccessToken(rawToken string) (TokenClaims, error) {
	claims, err := s.tokens.ParseToken(rawToken, TokenTypeAccess)
	if err != nil {
		return TokenClaims{}, mapTokenError(err)
	}

	return claims, nil
}

func (s *Service) issueSessionTokens(ctx context.Context, account domain.Account, userAgent string, ip string) (AuthResult, error) {
	now := s.now().UTC()
	sessionID := s.idGenerator()

	refreshToken, err := s.tokens.IssueToken(account.ID, sessionID, TokenTypeRefresh, s.refreshTokenTTL, now)
	if err != nil {
		return AuthResult{}, fmt.Errorf("issue refresh token: %w", err)
	}

	session, err := domain.NewSession(sessionID, account.ID, refreshToken.Claims.TokenID, userAgent, ip, now, refreshToken.Claims.ExpiresAt)
	if err != nil {
		return AuthResult{}, mapDomainValidationError(err)
	}

	if err = s.sessions.CreateSession(ctx, session); err != nil {
		return AuthResult{}, err
	}

	accessToken, err := s.tokens.IssueToken(account.ID, sessionID, TokenTypeAccess, s.accessTokenTTL, now)
	if err != nil {
		return AuthResult{}, fmt.Errorf("issue access token: %w", err)
	}

	return AuthResult{
		AccountID:            account.ID,
		Email:                account.Email,
		SessionID:            sessionID,
		AccessToken:          accessToken.Raw,
		RefreshToken:         refreshToken.Raw,
		AccessTokenExpiresAt: accessToken.Claims.ExpiresAt,
		RefreshTokenExpireAt: refreshToken.Claims.ExpiresAt,
	}, nil
}

func validateAuthCommand(email string, password string) error {
	trimmedEmail := strings.TrimSpace(email)
	trimmedPassword := strings.TrimSpace(password)
	if trimmedEmail == "" || trimmedPassword == "" {
		return ErrInvalidCommand
	}

	if len(trimmedPassword) < 8 {
		return ErrInvalidCommand
	}

	return nil
}

func mapDomainValidationError(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalidEmail), errors.Is(err, domain.ErrInvalidPassword), errors.Is(err, domain.ErrInvalidIdentifier), errors.Is(err, domain.ErrInvalidTimestamp):
		return ErrInvalidCommand
	default:
		return err
	}
}

func mapTokenError(err error) error {
	switch {
	case errors.Is(err, ErrTokenExpired):
		return ErrTokenExpired
	case errors.Is(err, ErrTokenInvalid):
		return ErrTokenInvalid
	default:
		return ErrTokenInvalid
	}
}
