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
		"CREATE TABLE IF NOT EXISTS " + workspacesTable + " (workspace_id text PRIMARY KEY, name text, owner_id text, created_at timestamp, updated_at timestamp)",
		"CREATE TABLE IF NOT EXISTS " + channelsTable + " (workspace_id text, channel_id text, name text, kind text, created_at timestamp, updated_at timestamp, PRIMARY KEY ((workspace_id), channel_id))",
		"CREATE TABLE IF NOT EXISTS " + membersTable + " (workspace_id text, user_id text, joined_at timestamp, banned boolean, roles_json text, PRIMARY KEY ((workspace_id), user_id))",
		"CREATE TABLE IF NOT EXISTS " + rolesTable + " (workspace_id text, role_id text, name text, permissions_json text, created_at timestamp, updated_at timestamp, PRIMARY KEY ((workspace_id), role_id))",
		"CREATE TABLE IF NOT EXISTS " + overridesTable + " (workspace_id text, channel_id text, subject_type text, subject_id text, allow_mask bigint, deny_mask bigint, updated_at timestamp, PRIMARY KEY ((workspace_id, channel_id), subject_type, subject_id))",
		"CREATE TABLE IF NOT EXISTS " + processedCommandsTable + " (command_id text PRIMARY KEY, processed_at timestamp)",
		"CREATE TABLE IF NOT EXISTS " + outboxTable + " (event_id text PRIMARY KEY, event_type text, command_id text, correlation_id text, causation_id text, message_id text, occurred_at timestamp, workspace_id text, channel_id text, actor_id text, schema_version int, payload_json text, published boolean)",
		"CREATE INDEX IF NOT EXISTS " + outboxTable + "_published_idx ON " + outboxTable + " (published)",
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
