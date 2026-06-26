package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"

	"query-service/internal/cache"
	"query-service/internal/domain"
	"query-service/internal/store"
)

// LeaderboardStore is the read-only Redis query interface consumed by this handler.
type LeaderboardStore interface {
	TopN(ctx context.Context, gameID string, offset, count int64) ([]domain.RankedEntry, error)
	PlayerRank(ctx context.Context, gameID, playerID string) (rank int64, score float64, err error)
	Around(ctx context.Context, gameID, playerID string, radius int) ([]domain.RankedEntry, error)
}

type LeaderboardHandler struct {
	ls          LeaderboardStore
	cache       cache.Cache
	queryTotal  metric.Int64Counter
	queryDur    metric.Float64Histogram
	cacheTotal  metric.Int64Counter
}

func New(ls LeaderboardStore, c cache.Cache, meter metric.Meter) (*LeaderboardHandler, error) {
	qt, err := meter.Int64Counter("leaderboard_queries_total",
		metric.WithDescription("Total leaderboard queries by endpoint and status"))
	if err != nil {
		return nil, err
	}
	qd, err := meter.Float64Histogram("leaderboard_query_duration_seconds",
		metric.WithDescription("End-to-end query latency"),
		metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	ct, err := meter.Int64Counter("cache_lookups_total",
		metric.WithDescription("In-process cache lookups partitioned by result"),
	)
	if err != nil {
		return nil, err
	}
	return &LeaderboardHandler{ls: ls, cache: c, queryTotal: qt, queryDur: qd, cacheTotal: ct}, nil
}

// Register mounts all leaderboard routes onto mux.
func Register(mux *http.ServeMux, h *LeaderboardHandler) {
	// More specific patterns must be registered before less specific ones.
	mux.HandleFunc("GET /leaderboard/{game_id}/player/{player_id}", h.PlayerRank)
	mux.HandleFunc("GET /leaderboard/{game_id}/around/{player_id}", h.Around)
	mux.HandleFunc("GET /leaderboard/{game_id}", h.TopLeaderboard)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// — request/response types —

type topResponse struct {
	GameID  string              `json:"game_id"`
	Page    int                 `json:"page"`
	Size    int                 `json:"size"`
	Entries []domain.RankedEntry `json:"entries"`
}

type playerRankResponse struct {
	GameID   string  `json:"game_id"`
	PlayerID string  `json:"player_id"`
	Rank     int64   `json:"rank"`
	Score    float64 `json:"score"`
}

type aroundResponse struct {
	GameID   string              `json:"game_id"`
	PlayerID string              `json:"player_id"`
	Entries  []domain.RankedEntry `json:"entries"`
}

// — handlers —

func (h *LeaderboardHandler) TopLeaderboard(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	ctx, span := otel.Tracer("query-service").Start(ctx, "handler.TopLeaderboard")
	defer span.End()

	gameID := r.PathValue("game_id")
	page := queryInt(r, "page", 1)
	size := queryInt(r, "size", 50)
	if page < 1 {
		page = 1
	}
	if size < 1 || size > 100 {
		size = 50
	}

	span.SetAttributes(
		attribute.String("game.id", gameID),
		attribute.Int("page", page),
		attribute.Int("size", size),
	)

	cacheKey := fmt.Sprintf("%s:page=%d:size=%d", gameID, page, size)
	if cached := h.cache.Get(cacheKey); cached != nil {
		h.cacheTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "hit")))
		h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "top"), attribute.String("status", "ok")))
		h.queryDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("endpoint", "top")))
		span.SetStatus(codes.Ok, "")
		writeJSON(w, cached)
		return
	}
	h.cacheTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("result", "miss")))

	offset := int64((page - 1) * size)
	entries, err := h.ls.TopN(ctx, gameID, offset, int64(size))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "top"), attribute.String("status", "error")))
		jsonError(w, "leaderboard temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	resp := topResponse{GameID: gameID, Page: page, Size: size, Entries: entries}
	b, _ := json.Marshal(resp)
	h.cache.Set(cacheKey, b)

	span.SetStatus(codes.Ok, "")
	h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "top"), attribute.String("status", "ok")))
	h.queryDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("endpoint", "top")))
	writeJSON(w, b)
}

func (h *LeaderboardHandler) PlayerRank(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	ctx, span := otel.Tracer("query-service").Start(ctx, "handler.PlayerRank")
	defer span.End()

	gameID := r.PathValue("game_id")
	playerID := r.PathValue("player_id")
	span.SetAttributes(attribute.String("game.id", gameID), attribute.String("player.id", playerID))

	rank, score, err := h.ls.PlayerRank(ctx, gameID, playerID)
	if errors.Is(err, store.ErrNotFound) {
		span.SetStatus(codes.Ok, "not found")
		h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "rank"), attribute.String("status", "not_found")))
		jsonError(w, "player not ranked in this game", http.StatusNotFound)
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "rank"), attribute.String("status", "error")))
		jsonError(w, "leaderboard temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	resp := playerRankResponse{GameID: gameID, PlayerID: playerID, Rank: rank, Score: score}
	b, _ := json.Marshal(resp)

	span.SetStatus(codes.Ok, "")
	h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "rank"), attribute.String("status", "ok")))
	h.queryDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("endpoint", "rank")))
	writeJSON(w, b)
}

func (h *LeaderboardHandler) Around(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	ctx, span := otel.Tracer("query-service").Start(ctx, "handler.Around")
	defer span.End()

	gameID := r.PathValue("game_id")
	playerID := r.PathValue("player_id")
	radius := queryInt(r, "radius", 5)
	if radius < 1 || radius > 50 {
		radius = 5
	}

	span.SetAttributes(
		attribute.String("game.id", gameID),
		attribute.String("player.id", playerID),
		attribute.Int("radius", radius),
	)

	entries, err := h.ls.Around(ctx, gameID, playerID, radius)
	if errors.Is(err, store.ErrNotFound) {
		span.SetStatus(codes.Ok, "not found")
		h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "around"), attribute.String("status", "not_found")))
		jsonError(w, "player not ranked in this game", http.StatusNotFound)
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "around"), attribute.String("status", "error")))
		jsonError(w, "leaderboard temporarily unavailable", http.StatusServiceUnavailable)
		return
	}

	resp := aroundResponse{GameID: gameID, PlayerID: playerID, Entries: entries}
	b, _ := json.Marshal(resp)

	span.SetStatus(codes.Ok, "")
	h.queryTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("endpoint", "around"), attribute.String("status", "ok")))
	h.queryDur.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes(attribute.String("endpoint", "around")))
	writeJSON(w, b)
}

// — helpers —

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func writeJSON(w http.ResponseWriter, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(b) //nolint:errcheck
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}
