package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"ranking-service/internal/domain"
	"ranking-service/internal/ranking"
	"ranking-service/internal/registry"
)

type KafkaConsumer struct {
	client         *kgo.Client
	processor      *ranking.EventProcessor
	log            *slog.Logger
	registryClient *registry.SchemaRegistryClient
}

func New(brokers []string, topic, groupID string, registryURL string, processor *ranking.EventProcessor, log *slog.Logger) (*KafkaConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		// Manual commit so we only advance offsets after successful processing.
		// On Redis failure the batch is retried from the last committed offset.
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka consumer client: %w", err)
	}

	var regClient *registry.SchemaRegistryClient
	if registryURL != "" {
		regClient = registry.NewClient(registryURL)
	}

	return &KafkaConsumer{
		client:         client,
		processor:      processor,
		log:            log,
		registryClient: regClient,
	}, nil
}

// Run polls Kafka until ctx is cancelled. It commits offsets only after all
// records in a fetch are processed successfully. On infrastructure errors
// (Redis down) it pauses and retries without committing, allowing Kafka to
// replay the batch from the last committed offset on restart.
func (c *KafkaConsumer) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		fetches := c.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				c.log.Error("kafka fetch error", "error", fe)
			}
			time.Sleep(2 * time.Second)
			continue
		}

		var processingErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if processingErr != nil {
				return // stop the batch on first infrastructure error
			}
			if err := c.handleRecord(ctx, r); err != nil {
				processingErr = err
			}
		})

		if processingErr != nil {
			// Transient error (Redis unavailable) — don't commit so Kafka
			// replays this batch on the next poll or after a restart.
			c.log.Error("batch processing error, pausing before retry", "error", processingErr)
			time.Sleep(5 * time.Second)
			continue
		}

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("offset commit error", "error", err)
		}
	}
}

func (c *KafkaConsumer) handleRecord(ctx context.Context, r *kgo.Record) error {
	ctx, span := otel.Tracer("ranking-service").Start(ctx, "consumer.handleRecord")
	defer span.End()

	span.SetAttributes(
		semconv.MessagingSystemKafka,
		attribute.String("messaging.destination.name", r.Topic),
		attribute.Int64("messaging.kafka.offset", r.Offset),
		attribute.Int("messaging.kafka.partition", int(r.Partition)),
	)

	var event domain.ScoreSubmittedEvent
	var decoded bool
	var err error

	if c.registryClient != nil {
		decoded, err = c.registryClient.DecodeAvro(r.Value, &event)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "malformed avro event")
			c.log.Error("malformed avro record, skipping", "topic", r.Topic, "offset", r.Offset, "error", err)
			return nil
		}
		if decoded {
			span.SetAttributes(attribute.String("messaging.payload.format", "avro"))
		}
	}

	if !decoded {
		if err := json.Unmarshal(r.Value, &event); err != nil {
			// Malformed message — skip and let the consumer advance.
			// Retrying a corrupt payload will never succeed.
			span.RecordError(err)
			span.SetStatus(codes.Error, "malformed json event")
			c.log.Error("malformed json record, skipping", "topic", r.Topic, "offset", r.Offset, "error", err)
			return nil
		}
		span.SetAttributes(attribute.String("messaging.payload.format", "json"))
	}

	if err := c.processor.Process(ctx, &event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err // propagate to stop the batch and trigger retry
	}

	span.SetStatus(codes.Ok, "")
	return nil
}

func (c *KafkaConsumer) Close() {
	c.client.Close()
}
