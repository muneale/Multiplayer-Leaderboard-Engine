package config

import "os"

type Config struct {
	Port         string
	RedisAddr    string
	OTELEndpoint string
	ServiceName  string
}

func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3004"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	return Config{
		Port:         port,
		RedisAddr:    redisAddr,
		OTELEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:  "query-service",
	}
}
