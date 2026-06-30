package config

import (
	"os"
	"strings"
)

type Config struct {
	Port              string
	KafkaBrokers      []string
	KafkaTopic        string
	KafkaGroupID      string
	RedisAddr         string
	OTELEndpoint      string
	ServiceName       string
	SchemaRegistryURL string
}

func Load() Config {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9094"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "score-events"
	}
	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "ranking-service"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "3003"
	}
	schemaRegistryURL := os.Getenv("SCHEMA_REGISTRY_URL")
	return Config{
		Port:              port,
		KafkaBrokers:      strings.Split(brokers, ","),
		KafkaTopic:        topic,
		KafkaGroupID:      groupID,
		RedisAddr:         redisAddr,
		OTELEndpoint:      os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		ServiceName:       "ranking-service",
		SchemaRegistryURL: schemaRegistryURL,
	}
}
