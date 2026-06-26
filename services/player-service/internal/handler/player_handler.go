package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"player-service/internal/domain"
)

// PlayerStore is the persistence contract required by PlayerHandler.
// *repository.PlayerRepository satisfies it.
type PlayerStore interface {
	Create(ctx context.Context, username, region string) (*domain.Player, error)
	FindByID(ctx context.Context, id string) (*domain.Player, error)
}

type PlayerHandler struct {
	store PlayerStore
}

func NewPlayerHandler(store PlayerStore) *PlayerHandler {
	return &PlayerHandler{store: store}
}

type createPlayerRequest struct {
	Username string `json:"username"`
	Region   string `json:"region"`
}

func (h *PlayerHandler) CreatePlayer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := otel.Tracer("player-service").Start(ctx, "handler.CreatePlayer")
	defer span.End()

	var req createPlayerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	req.Username = strings.TrimSpace(req.Username)
	if req.Username == "" {
		jsonError(w, "username is required", http.StatusBadRequest)
		return
	}

	span.SetAttributes(attribute.String("player.username", req.Username))

	player, err := h.store.Create(ctx, req.Username, req.Region)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		jsonError(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(player) //nolint:errcheck
}

func (h *PlayerHandler) GetPlayer(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ctx, span := otel.Tracer("player-service").Start(ctx, "handler.GetPlayer")
	defer span.End()

	id := r.PathValue("id")
	span.SetAttributes(attribute.String("player.id", id))

	player, err := h.store.FindByID(ctx, id)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		jsonError(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if player == nil {
		jsonError(w, "player not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(player) //nolint:errcheck
}

// Register mounts all player-service routes onto mux.
func Register(mux *http.ServeMux, h *PlayerHandler) {
	mux.HandleFunc("POST /players", h.CreatePlayer)
	mux.HandleFunc("GET /players/{id}", h.GetPlayer)
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}
