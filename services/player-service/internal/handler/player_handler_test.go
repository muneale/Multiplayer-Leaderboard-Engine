package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"player-service/internal/domain"
	"player-service/internal/handler"
)

// mockStore is a hand-rolled test double for PlayerStore.
type mockStore struct {
	createFn   func(ctx context.Context, username, region string) (*domain.Player, error)
	findByIDFn func(ctx context.Context, id string) (*domain.Player, error)
}

func (m *mockStore) Create(ctx context.Context, username, region string) (*domain.Player, error) {
	return m.createFn(ctx, username, region)
}

func (m *mockStore) FindByID(ctx context.Context, id string) (*domain.Player, error) {
	return m.findByIDFn(ctx, id)
}

func newTestServer(store handler.PlayerStore) *http.ServeMux {
	mux := http.NewServeMux()
	handler.Register(mux, handler.NewPlayerHandler(store))
	return mux
}

func assertStatus(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("status = %d, want %d", got, want)
	}
}

func assertBodyContains(t *testing.T, body, substr string) {
	t.Helper()
	if !strings.Contains(body, substr) {
		t.Errorf("body %q does not contain %q", body, substr)
	}
}

var stubPlayer = &domain.Player{
	ID:        "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
	Username:  "alice",
	Region:    "EU",
	CreatedAt: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
}

// ── CreatePlayer ─────────────────────────────────────────────────────────────

func TestCreatePlayer_Valid(t *testing.T) {
	store := &mockStore{
		createFn: func(_ context.Context, username, region string) (*domain.Player, error) {
			return stubPlayer, nil
		},
	}
	mux := newTestServer(store)

	body := `{"username":"alice","region":"EU"}`
	req := httptest.NewRequest(http.MethodPost, "/players", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	assertStatus(t, rec.Code, http.StatusCreated)
	assertBodyContains(t, rec.Body.String(), `"id"`)
	assertBodyContains(t, rec.Body.String(), `"alice"`)

	var p domain.Player
	if err := json.NewDecoder(rec.Body).Decode(&p); err == nil {
		if p.ID != stubPlayer.ID {
			t.Errorf("id = %q, want %q", p.ID, stubPlayer.ID)
		}
	}
}

func TestCreatePlayer_NoRegion(t *testing.T) {
	store := &mockStore{
		createFn: func(_ context.Context, username, region string) (*domain.Player, error) {
			if region != "" {
				t.Errorf("expected empty region, got %q", region)
			}
			return &domain.Player{ID: "x", Username: username}, nil
		},
	}
	mux := newTestServer(store)

	req := httptest.NewRequest(http.MethodPost, "/players",
		strings.NewReader(`{"username":"bob"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusCreated)
}

func TestCreatePlayer_EmptyUsername(t *testing.T) {
	mux := newTestServer(&mockStore{})

	for _, body := range []string{
		`{"username":""}`,
		`{"username":"   "}`,
		`{}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/players", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		assertStatus(t, rec.Code, http.StatusBadRequest)
	}
}

func TestCreatePlayer_InvalidJSON(t *testing.T) {
	mux := newTestServer(&mockStore{})

	req := httptest.NewRequest(http.MethodPost, "/players", strings.NewReader(`not-json`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusBadRequest)
}

func TestCreatePlayer_StoreError(t *testing.T) {
	store := &mockStore{
		createFn: func(_ context.Context, _, _ string) (*domain.Player, error) {
			return nil, errors.New("db connection lost")
		},
	}
	mux := newTestServer(store)

	req := httptest.NewRequest(http.MethodPost, "/players",
		strings.NewReader(`{"username":"alice"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusInternalServerError)
}

// ── GetPlayer ─────────────────────────────────────────────────────────────────

func TestGetPlayer_Found(t *testing.T) {
	store := &mockStore{
		findByIDFn: func(_ context.Context, id string) (*domain.Player, error) {
			if id != stubPlayer.ID {
				t.Errorf("FindByID called with id=%q, want %q", id, stubPlayer.ID)
			}
			return stubPlayer, nil
		},
	}
	mux := newTestServer(store)

	req := httptest.NewRequest(http.MethodGet, "/players/"+stubPlayer.ID, nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	assertStatus(t, rec.Code, http.StatusOK)
	assertBodyContains(t, rec.Body.String(), stubPlayer.ID)
	assertBodyContains(t, rec.Body.String(), "alice")
}

func TestGetPlayer_NotFound(t *testing.T) {
	store := &mockStore{
		findByIDFn: func(_ context.Context, _ string) (*domain.Player, error) {
			return nil, nil
		},
	}
	mux := newTestServer(store)

	req := httptest.NewRequest(http.MethodGet, "/players/does-not-exist", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusNotFound)
	assertBodyContains(t, rec.Body.String(), "not found")
}

func TestGetPlayer_StoreError(t *testing.T) {
	store := &mockStore{
		findByIDFn: func(_ context.Context, _ string) (*domain.Player, error) {
			return nil, errors.New("redis timeout")
		},
	}
	mux := newTestServer(store)

	req := httptest.NewRequest(http.MethodGet, "/players/any-id", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusInternalServerError)
}

// ── Health ───────────────────────────────────────────────────────────────────

func TestHealth(t *testing.T) {
	mux := newTestServer(&mockStore{})

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)
	assertStatus(t, rec.Code, http.StatusOK)
}
