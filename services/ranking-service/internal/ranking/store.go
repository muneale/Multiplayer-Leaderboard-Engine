package ranking

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RankStore is the persistence contract for ranking operations.
// The concrete implementation targets Redis; tests use an in-memory stub.
type RankStore interface {
	// IsNewEvent atomically claims eventID with a 1-hour TTL.
	// Returns true when the event is new; false when it has already been processed.
	IsNewEvent(ctx context.Context, eventID string) (bool, error)
	// UpdateScore applies ZADD leaderboard:{gameID} GT score playerID.
	UpdateScore(ctx context.Context, gameID, playerID string, score float64) error
}

type RedisRankStore struct {
	rdb *redis.Client
}

func NewRedisRankStore(rdb *redis.Client) *RedisRankStore {
	return &RedisRankStore{rdb: rdb}
}

func (s *RedisRankStore) IsNewEvent(ctx context.Context, eventID string) (bool, error) {
	// SET NX is an atomic check-and-set — no read-then-write race.
	// Per-event key (not a shared Set) gives each event its own TTL,
	// so old dedup entries expire independently without resetting others.
	ok, err := s.rdb.SetNX(ctx, "dedup:score-events:"+eventID, "1", time.Hour).Result()
	if err != nil {
		return false, fmt.Errorf("dedup check: %w", err)
	}
	return ok, nil
}

func (s *RedisRankStore) UpdateScore(ctx context.Context, gameID, playerID string, score float64) error {
	// GT flag: only update if the new score is greater than the current one.
	// This makes ZADD idempotent for redelivered events — a player's score
	// can only ever move up, so reprocessing the same event is a no-op.
	err := s.rdb.ZAddArgs(ctx, "leaderboard:"+gameID, redis.ZAddArgs{
		GT:      true,
		Members: []redis.Z{{Score: score, Member: playerID}},
	}).Err()
	if err != nil {
		return fmt.Errorf("zadd leaderboard:%s: %w", gameID, err)
	}
	return nil
}
