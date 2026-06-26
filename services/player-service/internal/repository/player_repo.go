package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"player-service/internal/domain"
)

const playerExistsTTL = 10 * time.Minute

type PlayerRepository struct {
	db    *sql.DB
	redis *redis.Client
}

func NewPlayerRepository(db *sql.DB, rdb *redis.Client) *PlayerRepository {
	return &PlayerRepository{db: db, redis: rdb}
}

func (r *PlayerRepository) Create(ctx context.Context, username, region string) (*domain.Player, error) {
	ctx, span := otel.Tracer("player-service").Start(ctx, "PlayerRepository.Create")
	defer span.End()

	span.SetAttributes(attribute.String("player.username", username))

	var (
		id        string
		createdAt time.Time
		regionOut sql.NullString
	)

	err := r.db.QueryRowContext(ctx,
		`INSERT INTO players (username, region)
		 VALUES ($1, NULLIF($2, ''))
		 RETURNING id, region, created_at`,
		username, region,
	).Scan(&id, &regionOut, &createdAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("insert player: %w", err)
	}

	span.SetAttributes(attribute.String("player.id", id))

	p := &domain.Player{
		ID:        id,
		Username:  username,
		Region:    regionOut.String,
		CreatedAt: createdAt,
	}

	// Write existence key consumed by Score Service on its hot path.
	// Non-fatal: the Score Service falls back to a DB lookup on a cache miss.
	if err := r.redis.Set(ctx, "player:exists:"+id, "1", playerExistsTTL).Err(); err != nil {
		span.AddEvent("redis cache write failed", trace.WithAttributes(attribute.String("error", err.Error())))
	}

	return p, nil
}

func (r *PlayerRepository) FindByID(ctx context.Context, id string) (*domain.Player, error) {
	ctx, span := otel.Tracer("player-service").Start(ctx, "PlayerRepository.FindByID")
	defer span.End()

	span.SetAttributes(attribute.String("player.id", id))

	var (
		username  string
		regionOut sql.NullString
		createdAt time.Time
	)

	err := r.db.QueryRowContext(ctx,
		`SELECT username, region, created_at FROM players WHERE id = $1`, id,
	).Scan(&username, &regionOut, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("find player: %w", err)
	}

	return &domain.Player{
		ID:        id,
		Username:  username,
		Region:    regionOut.String,
		CreatedAt: createdAt,
	}, nil
}
