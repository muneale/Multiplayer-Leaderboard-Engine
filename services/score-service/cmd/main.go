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

	"score-service/internal/config"
	"score-service/internal/handler"
	"score-service/internal/publisher"
	"score-service/internal/telemetry"
	"score-service/internal/validator"
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

	pub, err := publisher.New(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		log.Error("failed to create kafka publisher", "error", err)
		rdb.Close()
		os.Exit(1)
	}

	val := validator.NewRedisPlayerValidator(rdb)

	meter := otel.GetMeterProvider().Meter(cfg.ServiceName)
	h, err := handler.New(val, pub, meter)
	if err != nil {
		log.Error("failed to create score handler", "error", err)
		pub.Close()
		rdb.Close()
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
		log.Info("score service listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, starting graceful shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Shut down HTTP Server
	log.Info("shutting down HTTP server...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful server shutdown failed", "error", err)
	} else {
		log.Info("HTTP server stopped successfully")
	}

	// 2. Close Kafka publisher (flushes buffered messages)
	log.Info("closing Kafka publisher...")
	pub.Close()
	log.Info("Kafka publisher closed successfully")

	// 3. Close Redis client
	log.Info("closing Redis client...")
	if err := rdb.Close(); err != nil {
		log.Error("failed to close Redis client", "error", err)
	} else {
		log.Info("Redis client closed successfully")
	}
}
