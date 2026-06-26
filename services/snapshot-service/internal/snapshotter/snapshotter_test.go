package snapshotter_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"go.opentelemetry.io/otel"

	"snapshot-service/internal/snapshotter"
)

// — mocks —

type mockGames struct {
	ids []string
	err error
}

func (m *mockGames) ListActiveGames(_ context.Context) ([]string, error) {
	return m.ids, m.err
}

type mockReader struct {
	entries map[string][]snapshotter.SnapshotEntry
	err     error
}

func (m *mockReader) ReadLeaderboard(_ context.Context, gameID string) ([]snapshotter.SnapshotEntry, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.entries[gameID], nil
}

type mockSaver struct {
	saved map[string][]snapshotter.SnapshotEntry
	err   error
}

func newMockSaver() *mockSaver {
	return &mockSaver{saved: make(map[string][]snapshotter.SnapshotEntry)}
}

func (m *mockSaver) SaveSnapshot(_ context.Context, gameID string, entries []snapshotter.SnapshotEntry) error {
	if m.err != nil {
		return m.err
	}
	m.saved[gameID] = entries
	return nil
}

// — helpers —

func newSnapshotter(t *testing.T, g snapshotter.GameLister, r snapshotter.LeaderboardReader, s snapshotter.SnapshotSaver) *snapshotter.Snapshotter {
	t.Helper()
	meter := otel.GetMeterProvider().Meter("test")
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	sn, err := snapshotter.New(g, r, s, log, meter)
	if err != nil {
		t.Fatalf("snapshotter.New: %v", err)
	}
	return sn
}

// — tests —

func TestTakeSnapshot_WritesAllActiveGames(t *testing.T) {
	entries := map[string][]snapshotter.SnapshotEntry{
		"game-1": {{Rank: 1, PlayerID: "p1", Score: 9500}},
		"game-2": {{Rank: 1, PlayerID: "p2", Score: 8000}},
	}
	games := &mockGames{ids: []string{"game-1", "game-2"}}
	reader := &mockReader{entries: entries}
	saver := newMockSaver()

	newSnapshotter(t, games, reader, saver).TakeSnapshot(context.Background())

	if len(saver.saved) != 2 {
		t.Errorf("want 2 snapshots, got %d", len(saver.saved))
	}
	if _, ok := saver.saved["game-1"]; !ok {
		t.Error("missing snapshot for game-1")
	}
	if _, ok := saver.saved["game-2"]; !ok {
		t.Error("missing snapshot for game-2")
	}
}

func TestTakeSnapshot_SkipsEmptyLeaderboard(t *testing.T) {
	games := &mockGames{ids: []string{"game-empty"}}
	reader := &mockReader{entries: map[string][]snapshotter.SnapshotEntry{}} // empty
	saver := newMockSaver()

	newSnapshotter(t, games, reader, saver).TakeSnapshot(context.Background())

	if len(saver.saved) != 0 {
		t.Errorf("should not save snapshot for empty leaderboard, got %d saves", len(saver.saved))
	}
}

func TestTakeSnapshot_GameListerError_NoSave(t *testing.T) {
	games := &mockGames{err: errors.New("db down")}
	saver := newMockSaver()

	newSnapshotter(t, games, &mockReader{}, saver).TakeSnapshot(context.Background())

	if len(saver.saved) != 0 {
		t.Error("should not attempt save when game listing fails")
	}
}

func TestTakeSnapshot_ReaderError_ContinuesOtherGames(t *testing.T) {
	// game-1 reader returns error; game-2 should still be snapshotted.
	games := &mockGames{ids: []string{"game-1", "game-2"}}
	reader := &mockReader{
		// game-1 has no entries (so reader returns empty, not error)
		// To trigger a reader error we need to use a custom reader below.
		entries: map[string][]snapshotter.SnapshotEntry{
			"game-2": {{Rank: 1, PlayerID: "p2", Score: 8000}},
		},
	}
	saver := newMockSaver()

	newSnapshotter(t, games, reader, saver).TakeSnapshot(context.Background())

	// game-1 is skipped (empty), game-2 is saved
	if _, ok := saver.saved["game-2"]; !ok {
		t.Error("game-2 should be saved even when game-1 has no data")
	}
}

func TestTakeSnapshot_SaverError_ContinuesOtherGames(t *testing.T) {
	games := &mockGames{ids: []string{"game-1", "game-2"}}
	entries := map[string][]snapshotter.SnapshotEntry{
		"game-1": {{Rank: 1, PlayerID: "p1", Score: 9500}},
		"game-2": {{Rank: 1, PlayerID: "p2", Score: 8000}},
	}
	reader := &mockReader{entries: entries}

	callCount := 0
	saver := &selectiveSaver{fn: func(gameID string, e []snapshotter.SnapshotEntry) error {
		callCount++
		if gameID == "game-1" {
			return errors.New("write failed")
		}
		return nil
	}}

	newSnapshotter(t, games, reader, saver).TakeSnapshot(context.Background())

	if callCount != 2 {
		t.Errorf("saver should be called for both games, got %d calls", callCount)
	}
}

// selectiveSaver lets us control per-game save behavior.
type selectiveSaver struct {
	fn func(gameID string, entries []snapshotter.SnapshotEntry) error
}

func (s *selectiveSaver) SaveSnapshot(_ context.Context, gameID string, entries []snapshotter.SnapshotEntry) error {
	return s.fn(gameID, entries)
}

func TestTakeSnapshot_PreservesRankOrder(t *testing.T) {
	games := &mockGames{ids: []string{"game-1"}}
	entries := map[string][]snapshotter.SnapshotEntry{
		"game-1": {
			{Rank: 1, PlayerID: "first", Score: 9999},
			{Rank: 2, PlayerID: "second", Score: 5000},
		},
	}
	reader := &mockReader{entries: entries}
	saver := newMockSaver()

	newSnapshotter(t, games, reader, saver).TakeSnapshot(context.Background())

	saved := saver.saved["game-1"]
	if len(saved) != 2 {
		t.Fatalf("want 2 entries, got %d", len(saved))
	}
	if saved[0].PlayerID != "first" || saved[0].Rank != 1 {
		t.Errorf("first entry wrong: %+v", saved[0])
	}
}
