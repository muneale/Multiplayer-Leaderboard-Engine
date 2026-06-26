package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port             string
	DatabaseURL      string
	RedisAddr        string
	OTELEndpoint     string
	ServiceName      string
	SnapshotInterval time.Duration
}

func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3005"
	}

	interval := 5 * time.Minute
	if s := os.Getenv("SNAPSHOT_INTERVAL_SECONDS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			interval = time.Duration(n) * time.Second
		}
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	return Config{
		Port:             port,
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		RedisAddr:        redisAddr,
		OTELEndpoint:     os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:      "snapshot-service",
		SnapshotInterval: interval,
	}
}
