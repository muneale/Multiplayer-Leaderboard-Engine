package config

import "os"

type Config struct {
	Port         string
	DatabaseURL  string
	RedisAddr    string
	OTELEndpoint string
	ServiceName  string
}

func Load() Config {
	return Config{
		Port:         getEnv("PORT", "3001"),
		DatabaseURL:  getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/leaderboard?sslmode=disable"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		OTELEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		ServiceName:  "player-service",
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
