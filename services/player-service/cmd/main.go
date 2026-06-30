package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"player-service/internal/config"
	"player-service/internal/handler"
	"player-service/internal/outbox"
	"player-service/internal/repository"
	"player-service/internal/telemetry"
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

	db, err := sql.Open("postgres", cfg.DatabaseURL)
	if err != nil {
		log.Error("failed to open database connection", "error", err)
		os.Exit(1)
	}

	if err := waitForDB(ctx, db, log); err != nil {
		log.Error("database not ready", "error", err)
		db.Close()
		os.Exit(1)
	}
	log.Info("database ready")

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	// Create Outbox Relay to process dual-write events asynchronously and reliably
	relay, err := outbox.NewRelay(db, cfg.KafkaBrokers, cfg.KafkaTopic, log)
	if err != nil {
		log.Error("failed to create outbox relay", "error", err)
		rdb.Close()
		db.Close()
		os.Exit(1)
	}

	repo := repository.NewPlayerRepository(db, rdb)
	h := handler.NewPlayerHandler(repo)

	mux := http.NewServeMux()
	handler.Register(mux, h)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      otelhttp.NewHandler(mux, "player-service"),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("player service listening", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, starting graceful shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Shut down HTTP Server (stops accepting new connections, waits for active requests)
	log.Info("shutting down HTTP server...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful server shutdown failed", "error", err)
	} else {
		log.Info("HTTP server stopped successfully")
	}

	// 2. Wait for background Outbox Relay worker to stop
	log.Info("waiting for outbox relay worker to stop...")
	wg.Wait()
	log.Info("outbox relay worker stopped successfully")

	// 3. Close Outbox Relay Kafka client
	log.Info("closing outbox relay Kafka client...")
	relay.Close()
	log.Info("outbox relay Kafka client closed successfully")

	// 4. Close Redis client
	log.Info("closing Redis client...")
	if err := rdb.Close(); err != nil {
		log.Error("failed to close Redis client", "error", err)
	} else {
		log.Info("Redis client closed successfully")
	}

	// 5. Close Postgres DB connection
	log.Info("closing Postgres database connections...")
	if err := db.Close(); err != nil {
		log.Error("failed to close database connection", "error", err)
	} else {
		log.Info("Postgres database connection closed successfully")
	}
}

func waitForDB(ctx context.Context, db *sql.DB, log *slog.Logger) error {
	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		log.Info("waiting for database...")
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for database")
		case <-time.After(2 * time.Second):
		}
	}
}
