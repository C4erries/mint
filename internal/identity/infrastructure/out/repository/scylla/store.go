package scyllarepo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/c4erries/mint/internal/identity/domain"
)

const (
	defaultPort        = 9042
	defaultConsistency = "quorum"
)

const (
	accountsTable         = "core_identity_accounts"
	accountsByEmailTable  = "core_identity_accounts_by_email"
	sessionsTable         = "core_identity_sessions"
	sessionByRefreshTable = "core_identity_session_by_refresh"
)

type SessionFactory interface {
	CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error)
}

type defaultSessionFactory struct{}

func (defaultSessionFactory) CreateSession(cluster *gocql.ClusterConfig) (*gocql.Session, error) {
	return cluster.CreateSession()
}

type Options struct {
	Hosts            []string
	Port             int
	Keyspace         string
	Consistency      string
	AutoCreateSchema bool
	SessionFactory   SessionFactory
}

// Store persists identity account/session models in ScyllaDB.
type Store struct {
	session *gocql.Session
}

func NewStore(options Options) (*Store, error) {
	hosts := normalizeHosts(options.Hosts)
	if len(hosts) == 0 {
		return nil, fmt.Errorf("scylla hosts are required")
	}

	if strings.TrimSpace(options.Keyspace) == "" {
		return nil, fmt.Errorf("scylla keyspace is required")
	}

	if options.Port <= 0 {
		options.Port = defaultPort
	}

	if strings.TrimSpace(options.Consistency) == "" {
		options.Consistency = defaultConsistency
	}

	if options.SessionFactory == nil {
		options.SessionFactory = defaultSessionFactory{}
	}

	consistency, err := parseConsistency(options.Consistency)
	if err != nil {
		return nil, err
	}

	if options.AutoCreateSchema {
		if err = ensureKeyspace(options, hosts, consistency); err != nil {
			return nil, err
		}
	}

	session, err := createKeyspaceSession(options, hosts, consistency)
	if err != nil {
		return nil, err
	}

	if options.AutoCreateSchema {
		if err = ensureSchema(context.Background(), session); err != nil {
			session.Close()
			return nil, err
		}
	}

	return &Store{session: session}, nil
}

func (s *Store) Close() error {
	if s == nil || s.session == nil {
		return nil
	}

	s.session.Close()
	return nil
}

func (s *Store) Ping(ctx context.Context) error {
	if s == nil || s.session == nil {
		return fmt.Errorf("scylla session is not initialized")
	}

	var releaseVersion string
	if err := s.session.Query("SELECT release_version FROM system.local LIMIT 1").WithContext(ctx).Scan(&releaseVersion); err != nil {
		return fmt.Errorf("ping scylla: %w", err)
	}

	return nil
}

func (s *Store) CreateAccount(ctx context.Context, account domain.Account) error {
	batch := s.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		"INSERT INTO "+accountsTable+" (account_id, email, password_hash, created_at) VALUES (?, ?, ?, ?)",
		account.ID,
		account.Email,
		account.PasswordHash,
		account.CreatedAt,
	)
	batch.Query(
		"INSERT INTO "+accountsByEmailTable+" (email, account_id) VALUES (?, ?)",
		account.Email,
		account.ID,
	)

	if err := s.session.ExecuteBatch(batch); err != nil {
		if isAlreadyExistsError(err) {
			return fmt.Errorf("account already exists: %w", err)
		}

		return fmt.Errorf("insert identity account: %w", err)
	}

	return nil
}

func (s *Store) GetAccountByEmail(ctx context.Context, email string) (domain.Account, error) {
	var accountID string
	if err := s.session.Query("SELECT account_id FROM "+accountsByEmailTable+" WHERE email = ?", email).WithContext(ctx).Consistency(gocql.One).Scan(&accountID); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Account{}, domain.ErrAccountNotFound
		}

		return domain.Account{}, fmt.Errorf("query account by email: %w", err)
	}

	return s.GetAccountByID(ctx, accountID)
}

func (s *Store) GetAccountByID(ctx context.Context, accountID string) (domain.Account, error) {
	var account domain.Account
	if err := s.session.Query(
		"SELECT account_id, email, password_hash, created_at FROM "+accountsTable+" WHERE account_id = ?",
		accountID,
	).WithContext(ctx).Consistency(gocql.One).Scan(&account.ID, &account.Email, &account.PasswordHash, &account.CreatedAt); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Account{}, domain.ErrAccountNotFound
		}

		return domain.Account{}, fmt.Errorf("query account by id: %w", err)
	}

	return account, nil
}

func (s *Store) CreateSession(ctx context.Context, session domain.Session) error {
	batch := s.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		"INSERT INTO "+sessionsTable+" (session_id, account_id, refresh_jti, user_agent, ip, created_at, expires_at, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		session.ID,
		session.AccountID,
		session.RefreshJTI,
		session.UserAgent,
		session.IP,
		session.CreatedAt,
		session.ExpiresAt,
		nil,
	)
	batch.Query(
		"INSERT INTO "+sessionByRefreshTable+" (refresh_jti, session_id) VALUES (?, ?)",
		session.RefreshJTI,
		session.ID,
	)

	if err := s.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("insert identity session: %w", err)
	}

	return nil
}

func (s *Store) GetSessionByRefreshJTI(ctx context.Context, refreshJTI string) (domain.Session, error) {
	var sessionID string
	if err := s.session.Query(
		"SELECT session_id FROM "+sessionByRefreshTable+" WHERE refresh_jti = ?",
		refreshJTI,
	).WithContext(ctx).Consistency(gocql.One).Scan(&sessionID); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Session{}, domain.ErrSessionNotFound
		}

		return domain.Session{}, fmt.Errorf("query session by refresh index: %w", err)
	}

	var session domain.Session
	var revokedAt *time.Time
	if err := s.session.Query(
		"SELECT session_id, account_id, refresh_jti, user_agent, ip, created_at, expires_at, revoked_at FROM "+sessionsTable+" WHERE session_id = ?",
		sessionID,
	).WithContext(ctx).Consistency(gocql.One).Scan(
		&session.ID,
		&session.AccountID,
		&session.RefreshJTI,
		&session.UserAgent,
		&session.IP,
		&session.CreatedAt,
		&session.ExpiresAt,
		&revokedAt,
	); err != nil {
		if err == gocql.ErrNotFound {
			return domain.Session{}, domain.ErrSessionNotFound
		}

		return domain.Session{}, fmt.Errorf("query identity session by id: %w", err)
	}

	session.RevokedAt = revokedAt

	return session, nil
}

func (s *Store) UpdateSessionRefresh(ctx context.Context, sessionID string, oldRefreshJTI string, newRefreshJTI string, expiresAt time.Time) error {
	batch := s.session.Batch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		"UPDATE "+sessionsTable+" SET refresh_jti = ?, expires_at = ? WHERE session_id = ?",
		newRefreshJTI,
		expiresAt,
		sessionID,
	)
	batch.Query(
		"DELETE FROM "+sessionByRefreshTable+" WHERE refresh_jti = ?",
		oldRefreshJTI,
	)
	batch.Query(
		"INSERT INTO "+sessionByRefreshTable+" (refresh_jti, session_id) VALUES (?, ?)",
		newRefreshJTI,
		sessionID,
	)

	if err := s.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("update identity session refresh: %w", err)
	}

	return nil
}

func (s *Store) RevokeSession(ctx context.Context, sessionID string, revokedAt time.Time) error {
	if err := s.session.Query(
		"UPDATE "+sessionsTable+" SET revoked_at = ? WHERE session_id = ?",
		revokedAt,
		sessionID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("revoke identity session: %w", err)
	}

	return nil
}

func isAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}

	return strings.Contains(strings.ToLower(err.Error()), "already")
}
