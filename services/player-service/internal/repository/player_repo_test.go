//go:build integration

package repository_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"player-service/internal/repository"
)

// schema is inlined so tests don't rely on a relative path to the migrations file.
const schema = `
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE TABLE IF NOT EXISTS players (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username   VARCHAR(64) NOT NULL,
    region     VARCHAR(32),
    created_at TIMESTAMP NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(64) NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    processed  BOOLEAN NOT NULL DEFAULT false,
    processed_at TIMESTAMP
);
`

var (
	testDB    *sql.DB
	testRedis *redis.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	pgDSN, pgStop, err := startPostgres(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start postgres: %v\n", err)
		os.Exit(1)
	}
	defer pgStop()

	redisAddr, redisStop, err := startRedis(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis: %v\n", err)
		os.Exit(1)
	}
	defer redisStop()

	testDB, err = sql.Open("postgres", pgDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer testDB.Close()

	if _, err := testDB.Exec(schema); err != nil {
		fmt.Fprintf(os.Stderr, "apply schema: %v\n", err)
		os.Exit(1)
	}

	testRedis = redis.NewClient(&redis.Options{Addr: redisAddr})
	defer testRedis.Close()

	os.Exit(m.Run())
}

func startPostgres(ctx context.Context) (dsn string, stop func(), err error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_DB":       "testdb",
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
			},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("start postgres container: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", nil, err
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		return "", nil, err
	}

	dsn = fmt.Sprintf(
		"postgres://postgres:postgres@%s:%s/testdb?sslmode=disable",
		host, port.Port(),
	)
	stop = func() { _ = c.Terminate(ctx) }
	return dsn, stop, nil
}

func startRedis(ctx context.Context) (addr string, stop func(), err error) {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor: wait.ForLog("* Ready to accept connections").
				WithStartupTimeout(15 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return "", nil, fmt.Errorf("start redis container: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", nil, err
	}
	port, err := c.MappedPort(ctx, "6379")
	if err != nil {
		return "", nil, err
	}

	addr = fmt.Sprintf("%s:%s", host, port.Port())
	stop = func() { _ = c.Terminate(ctx) }
	return addr, stop, nil
}

// cleanPlayers truncates the players table between tests.
func cleanPlayers(t *testing.T) {
	t.Helper()
	if _, err := testDB.Exec("TRUNCATE TABLE players"); err != nil {
		t.Fatalf("truncate players: %v", err)
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestCreate_ReturnsPlayerWithID(t *testing.T) {
	cleanPlayers(t)
	repo := repository.NewPlayerRepository(testDB, testRedis)
	ctx := context.Background()

	p, err := repo.Create(ctx, "alice", "EU")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if p.ID == "" {
		t.Error("expected non-empty ID")
	}
	if p.Username != "alice" {
		t.Errorf("username = %q, want %q", p.Username, "alice")
	}
	if p.Region != "EU" {
		t.Errorf("region = %q, want %q", p.Region, "EU")
	}
	if p.CreatedAt.IsZero() {
		t.Error("expected non-zero created_at")
	}
}

func TestCreate_NoRegion(t *testing.T) {
	cleanPlayers(t)
	repo := repository.NewPlayerRepository(testDB, testRedis)
	ctx := context.Background()

	p, err := repo.Create(ctx, "bob", "")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if p.Region != "" {
		t.Errorf("region = %q, want empty", p.Region)
	}
}

func TestCreate_WritesRedisExistenceKey(t *testing.T) {
	cleanPlayers(t)
	repo := repository.NewPlayerRepository(testDB, testRedis)
	ctx := context.Background()

	p, err := repo.Create(ctx, "carol", "US")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	val, err := testRedis.Get(ctx, "player:exists:"+p.ID).Result()
	if err != nil {
		t.Fatalf("redis Get: %v", err)
	}
	if val != "1" {
		t.Errorf("redis value = %q, want %q", val, "1")
	}

	ttl, err := testRedis.TTL(ctx, "player:exists:"+p.ID).Result()
	if err != nil {
		t.Fatalf("redis TTL: %v", err)
	}
	if ttl <= 0 {
		t.Error("expected positive TTL on existence key")
	}
}

func TestFindByID_Found(t *testing.T) {
	cleanPlayers(t)
	repo := repository.NewPlayerRepository(testDB, testRedis)
	ctx := context.Background()

	created, err := repo.Create(ctx, "dave", "AS")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	found, err := repo.FindByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if found == nil {
		t.Fatal("expected player, got nil")
	}
	if found.ID != created.ID {
		t.Errorf("id = %q, want %q", found.ID, created.ID)
	}
	if found.Username != "dave" {
		t.Errorf("username = %q, want %q", found.Username, "dave")
	}
	if found.Region != "AS" {
		t.Errorf("region = %q, want %q", found.Region, "AS")
	}
}

func TestFindByID_NotFound(t *testing.T) {
	repo := repository.NewPlayerRepository(testDB, testRedis)
	ctx := context.Background()

	p, err := repo.FindByID(ctx, "00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if p != nil {
		t.Errorf("expected nil, got %+v", p)
	}
}

func TestCreate_WritesOutboxRecord(t *testing.T) {
	cleanPlayers(t)
	if _, err := testDB.Exec("TRUNCATE TABLE outbox"); err != nil {
		t.Fatalf("truncate outbox: %v", err)
	}

	repo := repository.NewPlayerRepository(testDB, testRedis)
	ctx := context.Background()

	p, err := repo.Create(ctx, "outbox-user", "EU")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	var (
		eventType string
		payload   string
		processed bool
	)
	err = testDB.QueryRow("SELECT event_type, payload::text, processed FROM outbox LIMIT 1").Scan(&eventType, &payload, &processed)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}

	if eventType != "player.created" {
		t.Errorf("eventType = %q, want %q", eventType, "player.created")
	}
	if processed {
		t.Error("expected processed to be false")
	}
	if !strings.Contains(payload, p.ID) {
		t.Errorf("expected payload to contain player ID %q, got %q", p.ID, payload)
	}
}
