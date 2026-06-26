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
	"go.opentelemetry.io/otel"

	"ranking-service/internal/config"
	"ranking-service/internal/consumer"
	"ranking-service/internal/ranking"
	"ranking-service/internal/telemetry"
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

	store := ranking.NewRedisRankStore(rdb)
	meter := otel.GetMeterProvider().Meter(cfg.ServiceName)
	processor, err := ranking.NewEventProcessor(store, meter)
	if err != nil {
		log.Error("failed to create event processor", "error", err)
		os.Exit(1)
	}

	cons, err := consumer.New(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroupID, processor, log)
	if err != nil {
		log.Error("failed to create kafka consumer", "error", err)
		os.Exit(1)
	}
	defer cons.Close()

	// Minimal HTTP server — Ranking Service has no API but exposes /health
	// so Docker Compose and orchestrators can confirm the process is alive.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("ranking service health endpoint", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("health server error", "error", err)
		}
	}()

	log.Info("starting kafka consumer", "topic", cfg.KafkaTopic, "group", cfg.KafkaGroupID)
	go cons.Run(ctx)

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("health server shutdown error", "error", err)
	}
}
