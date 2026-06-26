package snapshotter

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
)

// SnapshotEntry is one row in the ranked leaderboard at capture time.
type SnapshotEntry struct {
	Rank     int64   `json:"rank"`
	PlayerID string  `json:"player_id"`
	Score    float64 `json:"score"`
}

// GameLister returns the IDs of all active games that should be snapshotted.
type GameLister interface {
	ListActiveGames(ctx context.Context) ([]string, error)
}

// LeaderboardReader reads the full ranked leaderboard for one game from Redis.
type LeaderboardReader interface {
	ReadLeaderboard(ctx context.Context, gameID string) ([]SnapshotEntry, error)
}

// SnapshotSaver persists a captured leaderboard to durable storage (PostgreSQL).
type SnapshotSaver interface {
	SaveSnapshot(ctx context.Context, gameID string, entries []SnapshotEntry) error
}

// Snapshotter coordinates one full snapshot cycle across all active games.
type Snapshotter struct {
	games   GameLister
	reader  LeaderboardReader
	saver   SnapshotSaver
	log     *slog.Logger
	taken   metric.Int64Counter
	dur     metric.Float64Histogram
	entries metric.Int64Histogram
}

func New(games GameLister, reader LeaderboardReader, saver SnapshotSaver, log *slog.Logger, meter metric.Meter) (*Snapshotter, error) {
	taken, err := meter.Int64Counter("snapshots_taken_total",
		metric.WithDescription("Number of leaderboard snapshots written to PostgreSQL"))
	if err != nil {
		return nil, err
	}
	dur, err := meter.Float64Histogram("snapshot_duration_seconds",
		metric.WithDescription("Time to complete one full snapshot cycle"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	ent, err := meter.Int64Histogram("snapshot_entries",
		metric.WithDescription("Number of ranked entries captured per game snapshot"))
	if err != nil {
		return nil, err
	}
	return &Snapshotter{games: games, reader: reader, saver: saver, log: log, taken: taken, dur: dur, entries: ent}, nil
}

// Run takes a snapshot immediately on startup then repeats every interval
// until ctx is cancelled.
func (s *Snapshotter) Run(ctx context.Context, interval time.Duration) {
	s.TakeSnapshot(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.TakeSnapshot(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// TakeSnapshot is exported so the scheduler logic can be tested independently.
func (s *Snapshotter) TakeSnapshot(ctx context.Context) {
	ctx, span := otel.Tracer("snapshot-service").Start(ctx, "snapshotter.TakeSnapshot")
	defer span.End()

	start := time.Now()

	gameIDs, err := s.games.ListActiveGames(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		s.log.Error("failed to list active games", "error", err)
		return
	}

	span.SetAttributes(attribute.Int("games.count", len(gameIDs)))

	for _, gameID := range gameIDs {
		if err := s.snapshotGame(ctx, gameID); err != nil {
			// Log and continue — one failing game must not block the others.
			s.log.Error("snapshot failed for game", "game_id", gameID, "error", err)
		}
	}

	s.dur.Record(ctx, time.Since(start).Seconds())
	span.SetStatus(codes.Ok, "")
}

func (s *Snapshotter) snapshotGame(ctx context.Context, gameID string) error {
	ctx, span := otel.Tracer("snapshot-service").Start(ctx, "snapshotter.snapshotGame")
	defer span.End()
	span.SetAttributes(attribute.String("game.id", gameID))

	entries, err := s.reader.ReadLeaderboard(ctx, gameID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if len(entries) == 0 {
		// No data yet — skip rather than writing an empty snapshot.
		s.log.Info("leaderboard empty, skipping snapshot", "game_id", gameID)
		return nil
	}

	if err := s.saver.SaveSnapshot(ctx, gameID, entries); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	s.taken.Add(ctx, 1)
	s.entries.Record(ctx, int64(len(entries)), metric.WithAttributes(attribute.String("game.id", gameID)))
	s.log.Info("snapshot saved", "game_id", gameID, "entries", len(entries))
	span.SetStatus(codes.Ok, "")
	return nil
}
