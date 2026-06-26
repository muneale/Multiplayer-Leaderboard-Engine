package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"

	"score-service/internal/domain"
	"score-service/internal/handler"
)

// -- Mocks --

type mockValidator struct {
	exists bool
	err    error
}

func (m *mockValidator) Exists(_ context.Context, _ string) (bool, error) {
	return m.exists, m.err
}

type mockPublisher struct {
	called bool
	err    error
}

func (m *mockPublisher) Publish(_ context.Context, _ *domain.ScoreSubmittedEvent) error {
	m.called = true
	return m.err
}

// -- Helpers --

func newHandler(t *testing.T, v handler.PlayerValidator, p handler.ScorePublisher) *handler.ScoreHandler {
	t.Helper()
	meter := otel.GetMeterProvider().Meter("test")
	h, err := handler.New(v, p, meter)
	if err != nil {
		t.Fatalf("handler.New: %v", err)
	}
	return h
}

func newMux(t *testing.T, v handler.PlayerValidator, p handler.ScorePublisher) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	handler.Register(mux, newHandler(t, v, p))
	return mux
}

func postJSON(t *testing.T, mux http.Handler, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/scores", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec
}

func validBody() map[string]any {
	return map[string]any{
		"player_id": "player-uuid-123",
		"game_id":   "game-uuid-456",
		"score":     9999.5,
	}
}

// -- Tests --

func TestHealth(t *testing.T) {
	mux := newMux(t, &mockValidator{exists: true}, &mockPublisher{})
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("health: want 200, got %d", rec.Code)
	}
}

func TestSubmitScore_HappyPath(t *testing.T) {
	pub := &mockPublisher{}
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, pub), validBody())
	if rec.Code != http.StatusAccepted {
		t.Errorf("want 202, got %d — body: %s", rec.Code, rec.Body.String())
	}
	if !pub.called {
		t.Error("expected publisher to be called")
	}
}

func TestSubmitScore_WithTimestamp(t *testing.T) {
	ts := time.Now().UTC().Add(-5 * time.Minute)
	body := map[string]any{
		"player_id": "player-uuid-123",
		"game_id":   "game-uuid-456",
		"score":     1.0,
		"timestamp": ts.Format(time.RFC3339Nano),
	}
	pub := &mockPublisher{}
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, pub), body)
	if rec.Code != http.StatusAccepted {
		t.Errorf("want 202, got %d — body: %s", rec.Code, rec.Body.String())
	}
}

func TestSubmitScore_PlayerNotFound(t *testing.T) {
	rec := postJSON(t, newMux(t, &mockValidator{exists: false}, &mockPublisher{}), validBody())
	if rec.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d", rec.Code)
	}
}

func TestSubmitScore_ValidatorError(t *testing.T) {
	rec := postJSON(t, newMux(t, &mockValidator{err: errors.New("redis down")}, &mockPublisher{}), validBody())
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d", rec.Code)
	}
}

func TestSubmitScore_PublisherError(t *testing.T) {
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, &mockPublisher{err: errors.New("kafka down")}), validBody())
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("want 503, got %d", rec.Code)
	}
}

func TestSubmitScore_MissingPlayerID(t *testing.T) {
	body := map[string]any{"game_id": "game-uuid-456", "score": 1.0}
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, &mockPublisher{}), body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("want 422, got %d", rec.Code)
	}
}

func TestSubmitScore_MissingGameID(t *testing.T) {
	body := map[string]any{"player_id": "player-uuid-123", "score": 1.0}
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, &mockPublisher{}), body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("want 422, got %d", rec.Code)
	}
}

func TestSubmitScore_NegativeScore(t *testing.T) {
	body := map[string]any{"player_id": "player-uuid-123", "game_id": "game-uuid-456", "score": -1.0}
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, &mockPublisher{}), body)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("want 422, got %d", rec.Code)
	}
}

func TestSubmitScore_InvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/scores", bytes.NewBufferString("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	newMux(t, &mockValidator{exists: true}, &mockPublisher{}).ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rec.Code)
	}
}

func TestSubmitScore_ZeroScoreAllowed(t *testing.T) {
	body := map[string]any{"player_id": "player-uuid-123", "game_id": "game-uuid-456", "score": 0}
	rec := postJSON(t, newMux(t, &mockValidator{exists: true}, &mockPublisher{}), body)
	if rec.Code != http.StatusAccepted {
		t.Errorf("zero score should be accepted, got %d", rec.Code)
	}
}
