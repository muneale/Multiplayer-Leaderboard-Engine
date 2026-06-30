package main

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"

	"snapshot-service/internal/config"
	"snapshot-service/internal/snapshotter"
	"snapshot-service/internal/telemetry"
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
		log.Error("failed to open postgres connection", "error", err)
		os.Exit(1)
	}

	if err := waitForDB(ctx, db, log); err != nil {
		log.Error("database not ready", "error", err)
		db.Close()
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})

	pg := snapshotter.NewPostgresStore(db)
	rr := snapshotter.NewRedisLeaderboardReader(rdb)
	meter := otel.GetMeterProvider().Meter(cfg.ServiceName)

	sn, err := snapshotter.New(pg, rr, pg, log, meter)
	if err != nil {
		log.Error("failed to create snapshotter", "error", err)
		rdb.Close()
		db.Close()
		os.Exit(1)
	}

	// Health endpoint for Docker orchestration.
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
		log.Info("snapshot service health endpoint", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("health server error", "error", err)
		}
	}()

	var wg sync.WaitGroup
	log.Info("starting snapshot scheduler", "interval", cfg.SnapshotInterval.String())
	wg.Add(1)
	go func() {
		defer wg.Done()
		sn.Run(ctx, cfg.SnapshotInterval)
	}()

	<-ctx.Done()
	log.Info("shutdown signal received, starting graceful shutdown")

	// 1. Shut down Health HTTP server
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	log.Info("shutting down health HTTP server...")
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("health server shutdown error", "error", err)
	} else {
		log.Info("health server stopped successfully")
	}

	// 2. Wait for snapshot scheduler loop to exit (since ctx is cancelled, it will stop and return)
	log.Info("waiting for snapshot scheduler to stop...")
	wg.Wait()
	log.Info("snapshot scheduler stopped successfully")

	// 3. Close Redis client
	log.Info("closing Redis client...")
	if err := rdb.Close(); err != nil {
		log.Error("failed to close Redis client", "error", err)
	} else {
		log.Info("Redis client closed successfully")
	}

	// 4. Close Postgres DB connection
	log.Info("closing Postgres database connections...")
	if err := db.Close(); err != nil {
		log.Error("failed to close database connection", "error", err)
	} else {
		log.Info("Postgres database connection closed successfully")
	}
}

func waitForDB(ctx context.Context, db *sql.DB, log *slog.Logger) error {
	for i := range 10 {
		if err := db.PingContext(ctx); err == nil {
			return nil
		} else if i == 0 {
			log.Info("waiting for database...")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return db.PingContext(ctx)
}
