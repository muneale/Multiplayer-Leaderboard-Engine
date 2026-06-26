package validator

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type RedisPlayerValidator struct {
	rdb *redis.Client
}

func NewRedisPlayerValidator(rdb *redis.Client) *RedisPlayerValidator {
	return &RedisPlayerValidator{rdb: rdb}
}

// Exists checks the player existence cache written by Player Service on registration.
// A cache miss means either the player does not exist or the TTL expired — the Score
// Service treats both as invalid to avoid accepting orphaned score events.
func (v *RedisPlayerValidator) Exists(ctx context.Context, playerID string) (bool, error) {
	ctx, span := otel.Tracer("score-service").Start(ctx, "validator.PlayerExists")
	defer span.End()

	span.SetAttributes(attribute.String("player.id", playerID))

	n, err := v.rdb.Exists(ctx, "player:exists:"+playerID).Result()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return false, fmt.Errorf("redis exists check: %w", err)
	}

	exists := n > 0
	span.SetAttributes(attribute.Bool("player.exists", exists))
	return exists, nil
}
