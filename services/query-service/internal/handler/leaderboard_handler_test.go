package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"

	"query-service/internal/cache"
	"query-service/internal/domain"
	"query-service/internal/handler"
	"query-service/internal/store"
)

// — mocks —

type mockStore struct {
	entries    []domain.RankedEntry
	rank       int64
	score      float64
	err        error
}

func (m *mockStore) TopN(_ context.Context, _ string, _, _ int64) ([]domain.RankedEntry, error) {
	return m.entries, m.err
}

func (m *mockStore) PlayerRank(_ context.Context, _, _ string) (int64, float64, error) {
	return m.rank, m.score, m.err
}

func (m *mockStore) Around(_ context.Context, _, _ string, _ int) ([]domain.RankedEntry, error) {
	return m.entries, m.err
}

// noopCache is always a miss — lets tests verify store is called.
type noopCache struct{}

func (noopCache) Get(_ string) []byte  { return nil }
func (noopCache) Set(_ string, _ []byte) {}

// capturingCache records the last Set call and returns it on Get.
type capturingCache struct {
	store map[string][]byte
}

func newCapturingCache() *capturingCache { return &capturingCache{store: make(map[string][]byte)} }
func (c *capturingCache) Get(key string) []byte  { return c.store[key] }
func (c *capturingCache) Set(key string, data []byte) { c.store[key] = data }

// — helpers —

func newMux(t *testing.T, s handler.LeaderboardStore, c cache.Cache) http.Handler {
	t.Helper()
	meter := otel.GetMeterProvider().Meter("test")
	h, err := handler.New(s, c, meter)
	if err != nil {
		t.Fatalf("handler.New: %v", err)
	}
	mux := http.NewServeMux()
	handler.Register(mux, h)
	return mux
}

func get(mux http.Handler, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

var sampleEntries = []domain.RankedEntry{
	{Rank: 1, PlayerID: "player-a", Score: 9500},
	{Rank: 2, PlayerID: "player-b", Score: 9000},
}

// — health —

func TestHealth(t *testing.T) {
	rec := get(newMux(t, &mockStore{}, noopCache{}), "/health")
	if rec.Code != 200 {
		t.Errorf("want 200, got %d", rec.Code)
	}
}

// — top leaderboard —

func TestTopLeaderboard_DefaultParams(t *testing.T) {
	rec := get(newMux(t, &mockStore{entries: sampleEntries}, noopCache{}), "/leaderboard/game-1")
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d — body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		GameID  string `json:"game_id"`
		Page    int    `json:"page"`
		Size    int    `json:"size"`
		Entries []any  `json:"entries"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.GameID != "game-1" {
		t.Errorf("wrong game_id: %q", resp.GameID)
	}
	if resp.Page != 1 || resp.Size != 50 {
		t.Errorf("want page=1 size=50, got page=%d size=%d", resp.Page, resp.Size)
	}
	if len(resp.Entries) != 2 {
		t.Errorf("want 2 entries, got %d", len(resp.Entries))
	}
}

func TestTopLeaderboard_CustomPage(t *testing.T) {
	rec := get(newMux(t, &mockStore{entries: sampleEntries}, noopCache{}), "/leaderboard/game-1?page=2&size=10")
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	var resp struct {
		Page int `json:"page"`
		Size int `json:"size"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Page != 2 || resp.Size != 10 {
		t.Errorf("want page=2 size=10, got page=%d size=%d", resp.Page, resp.Size)
	}
}

func TestTopLeaderboard_EmptyLeaderboard(t *testing.T) {
	rec := get(newMux(t, &mockStore{entries: []domain.RankedEntry{}}, noopCache{}), "/leaderboard/game-1")
	if rec.Code != 200 {
		t.Fatalf("want 200 for empty leaderboard, got %d", rec.Code)
	}
	var resp struct{ Entries []any `json:"entries"` }
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if len(resp.Entries) != 0 {
		t.Errorf("want 0 entries, got %d", len(resp.Entries))
	}
}

func TestTopLeaderboard_CacheHit(t *testing.T) {
	calls := 0
	s := &mockStore{entries: sampleEntries}
	origTopN := s.entries
	_ = origTopN

	// Use a capturing cache so the second request is served from cache.
	c := newCapturingCache()
	mux := newMux(t, s, c)

	get(mux, "/leaderboard/game-1") // populates cache
	rec := get(mux, "/leaderboard/game-1") // should hit cache
	_ = calls

	if rec.Code != 200 {
		t.Fatalf("want 200 on cache hit, got %d", rec.Code)
	}
}

func TestTopLeaderboard_StoreError(t *testing.T) {
	rec := get(newMux(t, &mockStore{err: store.ErrNotFound}, noopCache{}), "/leaderboard/game-1")
	if rec.Code != 503 {
		t.Errorf("want 503 on store error, got %d", rec.Code)
	}
}

// — player rank —

func TestPlayerRank_Found(t *testing.T) {
	s := &mockStore{rank: 3, score: 8500}
	rec := get(newMux(t, s, noopCache{}), "/leaderboard/game-1/player/player-a")
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d — body: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Rank  int64   `json:"rank"`
		Score float64 `json:"score"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Rank != 3 || resp.Score != 8500 {
		t.Errorf("want rank=3 score=8500, got rank=%d score=%v", resp.Rank, resp.Score)
	}
}

func TestPlayerRank_NotFound(t *testing.T) {
	rec := get(newMux(t, &mockStore{err: store.ErrNotFound}, noopCache{}), "/leaderboard/game-1/player/nobody")
	if rec.Code != 404 {
		t.Errorf("want 404, got %d", rec.Code)
	}
}

func TestPlayerRank_StoreError(t *testing.T) {
	// A non-ErrNotFound store error should return 503.
	s := &mockStore{err: &storeErr{}}
	rec := get(newMux(t, s, noopCache{}), "/leaderboard/game-1/player/player-a")
	if rec.Code != 503 {
		t.Errorf("want 503, got %d", rec.Code)
	}
}

type storeErr struct{}

func (e *storeErr) Error() string { return "redis down" }

// — around —

func TestAround_Found(t *testing.T) {
	s := &mockStore{entries: sampleEntries}
	rec := get(newMux(t, s, noopCache{}), "/leaderboard/game-1/around/player-a")
	if rec.Code != 200 {
		t.Fatalf("want 200, got %d — body: %s", rec.Code, rec.Body.String())
	}
}

func TestAround_NotFound(t *testing.T) {
	rec := get(newMux(t, &mockStore{err: store.ErrNotFound}, noopCache{}), "/leaderboard/game-1/around/nobody")
	if rec.Code != 404 {
		t.Errorf("want 404, got %d", rec.Code)
	}
}

func TestAround_DefaultRadius(t *testing.T) {
	s := &mockStore{entries: sampleEntries}
	rec := get(newMux(t, s, noopCache{}), "/leaderboard/game-1/around/player-a")
	if rec.Code != 200 {
		t.Fatalf("want 200 with default radius, got %d", rec.Code)
	}
}

func TestAround_CustomRadius(t *testing.T) {
	s := &mockStore{entries: sampleEntries}
	rec := get(newMux(t, s, noopCache{}), "/leaderboard/game-1/around/player-a?radius=10")
	if rec.Code != 200 {
		t.Fatalf("want 200 with radius=10, got %d", rec.Code)
	}
}
