package config

import (
	"os"
	"strings"
)

type Config struct {
	Port              string
	KafkaBrokers      []string
	KafkaTopic        string
	RedisAddr         string
	OTELEndpoint      string
	ServiceName       string
	SchemaRegistryURL string
}

func Load() Config {
	return Config{
		Port:              getEnv("PORT", "3002"),
		KafkaBrokers:      strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094"), ","),
		KafkaTopic:        getEnv("KAFKA_TOPIC", "score-events"),
		RedisAddr:         getEnv("REDIS_ADDR", "localhost:6379"),
		OTELEndpoint:      getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		ServiceName:       "score-service",
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", ""),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
