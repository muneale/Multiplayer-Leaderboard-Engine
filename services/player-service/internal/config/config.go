package config

import (
	"os"
	"strings"
)

type Config struct {
	Port         string
	DatabaseURL  string
	RedisAddr    string
	OTELEndpoint string
	ServiceName  string
	KafkaBrokers []string
	KafkaTopic   string
}

func Load() Config {
	return Config{
		Port:         getEnv("PORT", "3001"),
		DatabaseURL:  getEnv("DATABASE_URL", ""),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
		OTELEndpoint: getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		ServiceName:  "player-service",
		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094"), ","),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "player-events"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
