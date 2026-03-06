package scyllarepo

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

func ensureKeyspace(options Options, hosts []string, consistency gocql.Consistency) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = options.Port
	cluster.Consistency = consistency

	session, err := options.SessionFactory.CreateSession(cluster)
	if err != nil {
		return fmt.Errorf("create scylla admin session: %w", err)
	}
	defer session.Close()

	statement := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy','replication_factor':1}",
		options.Keyspace,
	)

	if err := session.Query(statement).Exec(); err != nil {
		return fmt.Errorf("create scylla keyspace: %w", err)
	}

	return nil
}

func createKeyspaceSession(options Options, hosts []string, consistency gocql.Consistency) (*gocql.Session, error) {
	cluster := gocql.NewCluster(hosts...)
	cluster.Port = options.Port
	cluster.Keyspace = options.Keyspace
	cluster.Consistency = consistency

	session, err := options.SessionFactory.CreateSession(cluster)
	if err != nil {
		return nil, fmt.Errorf("create scylla keyspace session: %w", err)
	}

	return session, nil
}

func ensureSchema(ctx context.Context, session *gocql.Session) error {
	statements := []string{
		"CREATE TABLE IF NOT EXISTS " + accountsTable + " (account_id text PRIMARY KEY, email text, password_hash text, created_at timestamp)",
		"CREATE TABLE IF NOT EXISTS " + accountsByEmailTable + " (email text PRIMARY KEY, account_id text)",
		"CREATE TABLE IF NOT EXISTS " + sessionsTable + " (session_id text PRIMARY KEY, account_id text, refresh_jti text, user_agent text, ip text, created_at timestamp, expires_at timestamp, revoked_at timestamp)",
		"CREATE TABLE IF NOT EXISTS " + sessionByRefreshTable + " (refresh_jti text PRIMARY KEY, session_id text)",
	}

	for _, statement := range statements {
		if err := session.Query(statement).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("execute schema statement: %w", err)
		}
	}

	return nil
}

func parseConsistency(raw string) (gocql.Consistency, error) {
	normalized := strings.ToUpper(strings.TrimSpace(raw))
	switch normalized {
	case "ANY":
		return gocql.Any, nil
	case "ONE":
		return gocql.One, nil
	case "TWO":
		return gocql.Two, nil
	case "THREE":
		return gocql.Three, nil
	case "QUORUM":
		return gocql.Quorum, nil
	case "ALL":
		return gocql.All, nil
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum, nil
	case "EACH_QUORUM":
		return gocql.EachQuorum, nil
	case "LOCAL_ONE":
		return gocql.LocalOne, nil
	default:
		return 0, fmt.Errorf("unsupported scylla consistency %q", raw)
	}
}

func normalizeHosts(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}

		result = append(result, trimmed)
	}

	return result
}
