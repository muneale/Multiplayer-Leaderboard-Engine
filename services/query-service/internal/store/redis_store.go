package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"query-service/internal/domain"
)

// ErrNotFound is returned when a player has no entry in a leaderboard.
var ErrNotFound = errors.New("player not found in leaderboard")

type RedisLeaderboardStore struct {
	rdb *redis.Client
}

func NewRedisLeaderboardStore(rdb *redis.Client) *RedisLeaderboardStore {
	return &RedisLeaderboardStore{rdb: rdb}
}

// TopN returns count entries starting at offset, ordered by score descending.
// An empty leaderboard returns an empty slice, not an error.
func (s *RedisLeaderboardStore) TopN(ctx context.Context, gameID string, offset, count int64) ([]domain.RankedEntry, error) {
	members, err := s.rdb.ZRevRangeWithScores(ctx, "leaderboard:"+gameID, offset, offset+count-1).Result()
	if err != nil {
		return nil, fmt.Errorf("zrevrange leaderboard:%s: %w", gameID, err)
	}
	entries := make([]domain.RankedEntry, len(members))
	for i, m := range members {
		entries[i] = domain.RankedEntry{
			Rank:     offset + int64(i) + 1,
			PlayerID: m.Member.(string),
			Score:    m.Score,
		}
	}
	return entries, nil
}

// PlayerRank returns the 1-based rank and score for a player.
// Returns ErrNotFound if the player has no entry.
func (s *RedisLeaderboardStore) PlayerRank(ctx context.Context, gameID, playerID string) (rank int64, score float64, err error) {
	key := "leaderboard:" + gameID

	r, err := s.rdb.ZRevRank(ctx, key, playerID).Result()
	if errors.Is(err, redis.Nil) {
		return 0, 0, ErrNotFound
	}
	if err != nil {
		return 0, 0, fmt.Errorf("zrevrank leaderboard:%s: %w", gameID, err)
	}

	sc, err := s.rdb.ZScore(ctx, key, playerID).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("zscore leaderboard:%s: %w", gameID, err)
	}

	return r + 1, sc, nil
}

// Around returns entries in a window of ±radius around the given player.
// Returns ErrNotFound if the player has no entry.
func (s *RedisLeaderboardStore) Around(ctx context.Context, gameID, playerID string, radius int) ([]domain.RankedEntry, error) {
	key := "leaderboard:" + gameID

	rank, err := s.rdb.ZRevRank(ctx, key, playerID).Result()
	if errors.Is(err, redis.Nil) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("zrevrank leaderboard:%s: %w", gameID, err)
	}

	start := rank - int64(radius)
	if start < 0 {
		start = 0
	}
	stop := rank + int64(radius)

	members, err := s.rdb.ZRevRangeWithScores(ctx, key, start, stop).Result()
	if err != nil {
		return nil, fmt.Errorf("zrevrange around leaderboard:%s: %w", gameID, err)
	}

	entries := make([]domain.RankedEntry, len(members))
	for i, m := range members {
		entries[i] = domain.RankedEntry{
			Rank:     start + int64(i) + 1,
			PlayerID: m.Member.(string),
			Score:    m.Score,
		}
	}
	return entries, nil
}
