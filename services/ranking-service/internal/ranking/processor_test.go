package ranking_test

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"

	"ranking-service/internal/domain"
	"ranking-service/internal/ranking"
)

// -- mock --

type mockStore struct {
	isNew     bool
	isNewErr  error
	updateErr error
	updated   bool
}

func (m *mockStore) IsNewEvent(_ context.Context, _ string) (bool, error) {
	return m.isNew, m.isNewErr
}

func (m *mockStore) UpdateScore(_ context.Context, _, _ string, _ float64) error {
	m.updated = true
	return m.updateErr
}

// -- helpers --

func newProcessor(t *testing.T, store ranking.RankStore) *ranking.EventProcessor {
	t.Helper()
	p, err := ranking.NewEventProcessor(store, otel.GetMeterProvider().Meter("test"))
	if err != nil {
		t.Fatalf("NewEventProcessor: %v", err)
	}
	return p
}

var evt = &domain.ScoreSubmittedEvent{
	EventID:  "evt-abc-001",
	PlayerID: "player-001",
	GameID:   "game-001",
	Score:    9500,
}

// -- tests --

func TestProcess_NewEvent_CallsUpdateScore(t *testing.T) {
	store := &mockStore{isNew: true}
	if err := newProcessor(t, store).Process(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !store.updated {
		t.Error("UpdateScore must be called for a new event")
	}
}

func TestProcess_DuplicateEvent_SkipsUpdate(t *testing.T) {
	store := &mockStore{isNew: false}
	if err := newProcessor(t, store).Process(context.Background(), evt); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if store.updated {
		t.Error("UpdateScore must NOT be called for a duplicate event")
	}
}

func TestProcess_IsNewError_ReturnsError(t *testing.T) {
	store := &mockStore{isNewErr: errors.New("redis down")}
	if err := newProcessor(t, store).Process(context.Background(), evt); err == nil {
		t.Error("expected error when dedup check fails")
	}
}

func TestProcess_UpdateError_ReturnsError(t *testing.T) {
	store := &mockStore{isNew: true, updateErr: errors.New("redis down")}
	if err := newProcessor(t, store).Process(context.Background(), evt); err == nil {
		t.Error("expected error when UpdateScore fails")
	}
}

func TestProcess_DuplicateEvent_ReturnsNil(t *testing.T) {
	// Duplicates are not errors — idempotent skip is the correct outcome.
	store := &mockStore{isNew: false}
	if err := newProcessor(t, store).Process(context.Background(), evt); err != nil {
		t.Errorf("duplicate event must return nil, got: %v", err)
	}
}
