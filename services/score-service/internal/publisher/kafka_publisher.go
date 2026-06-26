package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"score-service/internal/domain"
)

type KafkaPublisher struct {
	client *kgo.Client
	topic  string
}

func New(brokers []string, topic string) (*KafkaPublisher, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Wait for the partition leader to acknowledge before returning.
		// Balances durability and latency for a single-broker setup.
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client: %w", err)
	}
	return &KafkaPublisher{client: client, topic: topic}, nil
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

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	record := &kgo.Record{
		Topic: p.topic,
		// Partitioned by game_id so all events for one game go to the same
		// partition and are processed in order by a single Ranking Service instance.
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
