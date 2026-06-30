package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"score-service/internal/domain"
	"score-service/internal/registry"
)

const ScoreSubmittedEventSchema = `{
	"type": "record",
	"name": "ScoreSubmittedEvent",
	"namespace": "com.mune.leaderboard",
	"fields": [
		{"name": "event_id", "type": "string"},
		{"name": "player_id", "type": "string"},
		{"name": "game_id", "type": "string"},
		{"name": "score", "type": "double"},
		{
			"name": "timestamp",
			"type": {
				"type": "long",
				"logicalType": "timestamp-millis"
			}
		}
	]
}`

type KafkaPublisher struct {
	client         *kgo.Client
	topic          string
	registryClient *registry.SchemaRegistryClient
	avroSchema     avro.Schema
	schemaID       int
	registryMu     sync.Mutex
}

func New(brokers []string, topic string, registryURL string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Wait for all in-sync replicas to acknowledge (required for idempotent producer)
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}

	var regClient *registry.SchemaRegistryClient
	var parsedSchema avro.Schema
	if registryURL != "" {
		regClient = registry.NewClient(registryURL)
		parsedSchema, err = avro.Parse(ScoreSubmittedEventSchema)
		if err != nil {
			client.Close()
			return nil, fmt.Errorf("parse avro schema: %w", err)
		}
	}

	return &KafkaPublisher{
		client:         client,
		topic:          topic,
		registryClient: regClient,
		avroSchema:     parsedSchema,
	}, nil
}

func (p *KafkaPublisher) Publish(ctx context.Context, event *domain.ScoreSubmittedEvent) error {
	ctx, span := otel.Tracer("score-service").Start(ctx, "kafka.produce")
	defer span.End()

	span.SetAttributes(
		semconv.MessagingSystemKafka,
		attribute.String("messaging.destination.name", p.topic),
		attribute.String("messaging.kafka.message.key", event.GameID),
		attribute.String("messaging.message.id", event.EventID),
	)

	var data []byte
	var err error

	if p.registryClient != nil {
		// Lazy registration to tolerate Schema Registry starting up
		p.registryMu.Lock()
		if p.schemaID == 0 {
			id, err := p.registryClient.RegisterAvroSchema(p.topic+"-value", ScoreSubmittedEventSchema)
			if err != nil {
				p.registryMu.Unlock()
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return fmt.Errorf("register avro schema: %w", err)
			}
			p.schemaID = id
		}
		schemaID := p.schemaID
		p.registryMu.Unlock()

		data, err = p.registryClient.EncodeAvro(schemaID, p.avroSchema, event)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("avro encode: %w", err)
		}
		span.SetAttributes(attribute.String("messaging.payload.format", "avro"))
	} else {
		data, err = json.Marshal(event)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return fmt.Errorf("marshal event: %w", err)
		}
		span.SetAttributes(attribute.String("messaging.payload.format", "json"))
	}

	record := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(event.GameID),
		Value: data,
		Headers: []kgo.RecordHeader{
			{Key: "event_type", Value: []byte("score.submitted")},
			{Key: "event_id", Value: []byte(event.EventID)},
		},
	}

	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("produce to kafka: %w", err)
	}

	span.SetStatus(codes.Ok, "")
	return nil
}

func (p *KafkaPublisher) Close() {
	p.client.Close()
}
