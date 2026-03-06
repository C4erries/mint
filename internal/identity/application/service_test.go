package application

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/c4erries/mint/internal/identity/domain"
)

type inMemoryAccountRepo struct {
	byID    map[string]domain.Account
	byEmail map[string]string
}

func newInMemoryAccountRepo() *inMemoryAccountRepo {
	return &inMemoryAccountRepo{byID: make(map[string]domain.Account), byEmail: make(map[string]string)}
}

func (r *inMemoryAccountRepo) CreateAccount(_ context.Context, account domain.Account) error {
	if _, exists := r.byEmail[account.Email]; exists {
		return fmt.Errorf("duplicate account")
	}

	r.byID[account.ID] = account
	r.byEmail[account.Email] = account.ID
	return nil
}

func (r *inMemoryAccountRepo) GetAccountByEmail(_ context.Context, email string) (domain.Account, error) {
	accountID, ok := r.byEmail[strings.ToLower(strings.TrimSpace(email))]
	if !ok {
		return domain.Account{}, domain.ErrAccountNotFound
	}

	return r.byID[accountID], nil
}

func (r *inMemoryAccountRepo) GetAccountByID(_ context.Context, accountID string) (domain.Account, error) {
	account, ok := r.byID[accountID]
	if !ok {
		return domain.Account{}, domain.ErrAccountNotFound
	}

	return account, nil
}

type inMemorySessionRepo struct {
	byID         map[string]domain.Session
	byRefreshJTI map[string]string
}

func newInMemorySessionRepo() *inMemorySessionRepo {
	return &inMemorySessionRepo{byID: make(map[string]domain.Session), byRefreshJTI: make(map[string]string)}
}

func (r *inMemorySessionRepo) CreateSession(_ context.Context, session domain.Session) error {
	r.byID[session.ID] = session
	r.byRefreshJTI[session.RefreshJTI] = session.ID
	return nil
}

func (r *inMemorySessionRepo) GetSessionByRefreshJTI(_ context.Context, refreshJTI string) (domain.Session, error) {
	sessionID, ok := r.byRefreshJTI[refreshJTI]
	if !ok {
		return domain.Session{}, domain.ErrSessionNotFound
	}

	session, exists := r.byID[sessionID]
	if !exists {
		return domain.Session{}, domain.ErrSessionNotFound
	}

	return session, nil
}

func (r *inMemorySessionRepo) UpdateSessionRefresh(_ context.Context, sessionID string, oldRefreshJTI string, newRefreshJTI string, expiresAt time.Time) error {
	session, exists := r.byID[sessionID]
	if !exists {
		return domain.ErrSessionNotFound
	}

	delete(r.byRefreshJTI, oldRefreshJTI)
	session.RefreshJTI = newRefreshJTI
	session.ExpiresAt = expiresAt
	r.byID[sessionID] = session
	r.byRefreshJTI[newRefreshJTI] = sessionID
	return nil
}

func (r *inMemorySessionRepo) RevokeSession(_ context.Context, sessionID string, revokedAt time.Time) error {
	session, exists := r.byID[sessionID]
	if !exists {
		return domain.ErrSessionNotFound
	}

	session.RevokedAt = &revokedAt
	r.byID[sessionID] = session
	return nil
}

type inMemoryRevocationStore struct {
	revoked map[string]time.Time
}

func newInMemoryRevocationStore() *inMemoryRevocationStore {
	return &inMemoryRevocationStore{revoked: make(map[string]time.Time)}
}

func (s *inMemoryRevocationStore) MarkRevoked(_ context.Context, tokenID string, expiresAt time.Time) error {
	s.revoked[tokenID] = expiresAt
	return nil
}

func (s *inMemoryRevocationStore) IsRevoked(_ context.Context, tokenID string) (bool, error) {
	expiresAt, exists := s.revoked[tokenID]
	if !exists {
		return false, nil
	}

	return expiresAt.After(time.Now().UTC()), nil
}

func (s *inMemoryRevocationStore) Ping(_ context.Context) error {
	return nil
}

func (s *inMemoryRevocationStore) Close() error {
	return nil
}

type fakeHasher struct{}

func (fakeHasher) Hash(password string) (string, error) {
	return "hash:" + password, nil
}

func (fakeHasher) Compare(hashedPassword string, password string) error {
	expected := "hash:" + password
	if hashedPassword != expected {
		return errors.New("password mismatch")
	}

	return nil
}

type fakeTokenManager struct {
	counter int
}

func (m *fakeTokenManager) IssueToken(accountID string, sessionID string, tokenType TokenType, ttl time.Duration, now time.Time) (IssuedToken, error) {
	m.counter++
	tokenID := fmt.Sprintf("tok-%d", m.counter)
	expiresAt := now.Add(ttl)
	raw := strings.Join([]string{string(tokenType), tokenID, accountID, sessionID, strconv.FormatInt(expiresAt.Unix(), 10), strconv.FormatInt(now.Unix(), 10)}, "|")

	return IssuedToken{
		Raw: raw,
		Claims: TokenClaims{
			TokenID:   tokenID,
			AccountID: accountID,
			SessionID: sessionID,
			TokenType: tokenType,
			IssuedAt:  now,
			ExpiresAt: expiresAt,
		},
	}, nil
}

func (m *fakeTokenManager) ParseToken(rawToken string, expectedType TokenType) (TokenClaims, error) {
	parts := strings.Split(rawToken, "|")
	if len(parts) != 6 {
		return TokenClaims{}, ErrTokenInvalid
	}

	if parts[0] != string(expectedType) {
		return TokenClaims{}, ErrTokenInvalid
	}

	expiresAtUnix, err := strconv.ParseInt(parts[4], 10, 64)
	if err != nil {
		return TokenClaims{}, ErrTokenInvalid
	}

	issuedAtUnix, err := strconv.ParseInt(parts[5], 10, 64)
	if err != nil {
		return TokenClaims{}, ErrTokenInvalid
	}

	expiresAt := time.Unix(expiresAtUnix, 0).UTC()
	if !expiresAt.After(time.Now().UTC()) {
		return TokenClaims{}, ErrTokenExpired
	}

	return TokenClaims{
		TokenID:   parts[1],
		AccountID: parts[2],
		SessionID: parts[3],
		TokenType: expectedType,
		IssuedAt:  time.Unix(issuedAtUnix, 0).UTC(),
		ExpiresAt: expiresAt,
	}, nil
}

func newTestIdentityService(t *testing.T) (*Service, *inMemorySessionRepo, *inMemoryRevocationStore) {
	t.Helper()

	accounts := newInMemoryAccountRepo()
	sessions := newInMemorySessionRepo()
	revocations := newInMemoryRevocationStore()
	tokenManager := &fakeTokenManager{}

	clock := time.Now().UTC()
	service, err := NewService(accounts, sessions, revocations, fakeHasher{}, tokenManager, ServiceOptions{
		IDGenerator:     func() string { return fmt.Sprintf("id-%d", time.Now().UnixNano()) },
		Now:             func() time.Time { return clock },
		AccessTokenTTL:  15 * time.Minute,
		RefreshTokenTTL: 2 * time.Hour,
	})
	require.NoError(t, err)

	return service, sessions, revocations
}

func TestService_RegisterAndLogin(t *testing.T) {
	t.Parallel()

	service, _, _ := newTestIdentityService(t)

	testCases := []struct {
		name    string
		command LoginCommand
		err     error
	}{
		{
			name: "login with valid credentials",
			command: LoginCommand{
				Email:    "user@example.com",
				Password: "password123",
			},
		},
		{
			name: "login with invalid password",
			command: LoginCommand{
				Email:    "user@example.com",
				Password: "wrong-password",
			},
			err: ErrInvalidCredentials,
		},
	}

	registerResult, err := service.Register(context.Background(), RegisterCommand{
		Email:    "user@example.com",
		Password: "password123",
	})
	require.NoError(t, err)
	require.NotEmpty(t, registerResult.AccessToken)
	require.NotEmpty(t, registerResult.RefreshToken)

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			result, loginErr := service.Login(context.Background(), testCase.command)
			if testCase.err != nil {
				require.ErrorIs(t, loginErr, testCase.err)
				return
			}

			require.NoError(t, loginErr)
			require.NotEmpty(t, result.AccessToken)
			require.NotEmpty(t, result.RefreshToken)
		})
	}
}

func TestService_RefreshRotatesToken(t *testing.T) {
	t.Parallel()

	service, sessions, revocations := newTestIdentityService(t)

	registerResult, err := service.Register(context.Background(), RegisterCommand{
		Email:    "refresh@example.com",
		Password: "password123",
	})
	require.NoError(t, err)

	refreshResult, err := service.Refresh(context.Background(), RefreshCommand{RefreshToken: registerResult.RefreshToken})
	require.NoError(t, err)
	require.NotEqual(t, registerResult.RefreshToken, refreshResult.RefreshToken)

	_, oldRefreshErr := sessions.GetSessionByRefreshJTI(context.Background(), parseTokenID(registerResult.RefreshToken))
	require.ErrorIs(t, oldRefreshErr, domain.ErrSessionNotFound)

	revoked, err := revocations.IsRevoked(context.Background(), parseTokenID(registerResult.RefreshToken))
	require.NoError(t, err)
	require.True(t, revoked)
}

func TestService_LogoutRevokesSessionAndAccessToken(t *testing.T) {
	t.Parallel()

	service, sessions, revocations := newTestIdentityService(t)

	registerResult, err := service.Register(context.Background(), RegisterCommand{
		Email:    "logout@example.com",
		Password: "password123",
	})
	require.NoError(t, err)

	claims, err := service.ParseAccessToken(registerResult.AccessToken)
	require.NoError(t, err)

	err = service.Logout(context.Background(), LogoutCommand{AccessClaims: claims, RefreshToken: registerResult.RefreshToken})
	require.NoError(t, err)

	revoked, err := revocations.IsRevoked(context.Background(), claims.TokenID)
	require.NoError(t, err)
	require.True(t, revoked)

	session, sessionErr := sessions.GetSessionByRefreshJTI(context.Background(), parseTokenID(registerResult.RefreshToken))
	require.NoError(t, sessionErr)
	require.True(t, session.IsRevoked())
}

func parseTokenID(raw string) string {
	parts := strings.Split(raw, "|")
	if len(parts) < 2 {
		return ""
	}

	return parts[1]
}
