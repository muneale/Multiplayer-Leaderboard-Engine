package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"

	"query-service/internal/cache"
	"query-service/internal/config"
	"query-service/internal/handler"
	"query-service/internal/store"
	"query-service/internal/telemetry"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	otelShutdown, err := telemetry.Init(ctx, cfg.OTELEndpoint, cfg.ServiceName)
	if err != nil {
		log.Warn("telemetry unavailable, continuing without tracing", "error", err)
	} else {
		defer func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := otelShutdown(flushCtx); err != nil {
				log.Error("telemetry shutdown error", "error", err)
			}
		}()
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	ls := store.NewRedisLeaderboardStore(rdb)
	lc := cache.NewLocalCache(time.Second)
	meter := otel.GetMeterProvider().Meter(cfg.ServiceName)

	h, err := handler.New(ls, lc, meter)
	if err != nil {
		log.Error("failed to create leaderboard handler", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	handler.Register(mux, h)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      otelhttp.NewHandler(mux, cfg.ServiceName),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("query service listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
}
