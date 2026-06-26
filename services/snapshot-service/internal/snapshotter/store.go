package snapshotter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// PostgresStore implements GameLister and SnapshotSaver against the leaderboard DB.
type PostgresStore struct {
	db *sql.DB
}

func NewPostgresStore(db *sql.DB) *PostgresStore {
	return &PostgresStore{db: db}
}

func (s *PostgresStore) ListActiveGames(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM games WHERE active = true`)
	if err != nil {
		return nil, fmt.Errorf("list active games: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan game id: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *PostgresStore) SaveSnapshot(ctx context.Context, gameID string, entries []SnapshotEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("marshal snapshot data: %w", err)
	}

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO leaderboard_snapshots (game_id, data) VALUES ($1, $2)`,
		gameID, data,
	)
	if err != nil {
		return fmt.Errorf("insert leaderboard_snapshot: %w", err)
	}
	return nil
}

// RedisLeaderboardReader implements LeaderboardReader using ZREVRANGE WITHSCORES.
type RedisLeaderboardReader struct {
	rdb *redis.Client
}

func NewRedisLeaderboardReader(rdb *redis.Client) *RedisLeaderboardReader {
	return &RedisLeaderboardReader{rdb: rdb}
}

func (r *RedisLeaderboardReader) ReadLeaderboard(ctx context.Context, gameID string) ([]SnapshotEntry, error) {
	// 0 to -1 reads the entire sorted set.
	members, err := r.rdb.ZRevRangeWithScores(ctx, "leaderboard:"+gameID, 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("zrevrange leaderboard:%s: %w", gameID, err)
	}

	entries := make([]SnapshotEntry, len(members))
	for i, m := range members {
		entries[i] = SnapshotEntry{
			Rank:     int64(i) + 1,
			PlayerID: m.Member.(string),
			Score:    m.Score,
		}
	}
	return entries, nil
}
