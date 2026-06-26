package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"

	"score-service/internal/domain"
)

// PlayerValidator checks that a player exists before a score is accepted.
// The implementation reads the Redis key written by Player Service at registration.
type PlayerValidator interface {
	Exists(ctx context.Context, playerID string) (bool, error)
}

// ScorePublisher sends the score.submitted event to Kafka.
type ScorePublisher interface {
	Publish(ctx context.Context, event *domain.ScoreSubmittedEvent) error
}

type ScoreHandler struct {
	validator PlayerValidator
	publisher ScorePublisher
	counter   metric.Int64Counter
}

func New(v PlayerValidator, p ScorePublisher, meter metric.Meter) (*ScoreHandler, error) {
	counter, err := meter.Int64Counter(
		"score_submissions_total",
		metric.WithDescription("Total score submissions partitioned by outcome status"),
	)
	if err != nil {
		return nil, err
	}
	return &ScoreHandler{validator: v, publisher: p, counter: counter}, nil
}

type submitScoreRequest struct {
	PlayerID  string     `json:"player_id"`
	GameID    string     `json:"game_id"`
	Score     float64    `json:"score"`
	Timestamp *time.Time `json:"timestamp"`
}

func (r *submitScoreRequest) validate() error {
	r.PlayerID = strings.TrimSpace(r.PlayerID)
	r.GameID = strings.TrimSpace(r.GameID)
	if r.PlayerID == "" {
		return errors.New("player_id is required")
	}
	if r.GameID == "" {
		return errors.New("game_id is required")
	}
	if r.Score < 0 {
		return errors.New("score must not be negative")
	}
	return nil
}

func (h *ScoreHandler) SubmitScore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := otel.Tracer("score-service").Start(ctx, "handler.SubmitScore")
	defer span.End()

	var req submitScoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := req.validate(); err != nil {
		jsonError(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	span.SetAttributes(
		attribute.String("player.id", req.PlayerID),
		attribute.String("game.id", req.GameID),
		attribute.Float64("score.value", req.Score),
	)

	exists, err := h.validator.Exists(ctx, req.PlayerID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error")))
		jsonError(w, "player validation temporarily unavailable", http.StatusServiceUnavailable)
		return
	}
	if !exists {
		h.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "rejected")))
		jsonError(w, "player not found", http.StatusNotFound)
		return
	}

	ts := time.Now().UTC()
	if req.Timestamp != nil {
		ts = req.Timestamp.UTC()
	}

	event := &domain.ScoreSubmittedEvent{
		EventID:   uuid.New().String(),
		PlayerID:  req.PlayerID,
		GameID:    req.GameID,
		Score:     req.Score,
		Timestamp: ts,
	}

	if err := h.publisher.Publish(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error")))
		jsonError(w, "service temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	span.SetStatus(codes.Ok, "")
	h.counter.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "accepted")))
	w.WriteHeader(http.StatusAccepted)
}

// Register mounts all score-service routes onto mux.
func Register(mux *http.ServeMux, h *ScoreHandler) {
	mux.HandleFunc("POST /scores", h.SubmitScore)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}
