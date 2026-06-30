package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type OutboxEvent struct {
	ID        string
	EventType string
	Payload   []byte
}

type OutboxRelay struct {
	db          *sql.DB
	kafkaClient *kgo.Client
	topic       string
	log         *slog.Logger
}

func NewRelay(db *sql.DB, brokers []string, topic string, log *slog.Logger) (*OutboxRelay, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("new kafka client for outbox: %w", err)
	}

	return &OutboxRelay{
		db:          db,
		kafkaClient: client,
		topic:       topic,
		log:         log,
	}, nil
}

func (r *OutboxRelay) Run(ctx context.Context) {
	r.log.Info("starting outbox relay worker")
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.log.Info("stopping outbox relay worker")
			return
		case <-ticker.C:
			if err := r.processBatch(ctx); err != nil {
				r.log.Error("error processing outbox batch", "error", err)
			}
		}
	}
}

func (r *OutboxRelay) processBatch(ctx context.Context) error {
	// Query unprocessed outbox records
	rows, err := r.db.QueryContext(ctx,
		`SELECT id, event_type, payload FROM outbox 
		 WHERE processed = false 
		 ORDER BY created_at ASC 
		 LIMIT 50`,
	)
	if err != nil {
		return fmt.Errorf("query outbox: %w", err)
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var ev OutboxEvent
		if err := rows.Scan(&ev.ID, &ev.EventType, &ev.Payload); err != nil {
			return fmt.Errorf("scan outbox: %w", err)
		}
		events = append(events, ev)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows error: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	r.log.Info("found outbox events to process", "count", len(events))

	for _, ev := range events {
		// Publish event payload to Kafka
		record := &kgo.Record{
			Topic: r.topic,
			Value: ev.Payload,
			Headers: []kgo.RecordHeader{
				{Key: "event_type", Value: []byte(ev.EventType)},
				{Key: "event_id", Value: []byte(ev.ID)},
			},
		}

		if err := r.kafkaClient.ProduceSync(ctx, record).FirstErr(); err != nil {
			return fmt.Errorf("publish outbox event to kafka (id=%s): %w", ev.ID, err)
		}

		// Mark outbox record as processed in Postgres
		_, err = r.db.ExecContext(ctx,
			`UPDATE outbox SET processed = true, processed_at = now() WHERE id = $1`,
			ev.ID,
		)
		if err != nil {
			// Note: if Postgres fails here, the event could be published again on retry (at-least-once delivery)
			return fmt.Errorf("mark outbox as processed (id=%s): %w", ev.ID, err)
		}

		r.log.Info("successfully processed and relayed outbox event", "event_id", ev.ID, "event_type", ev.EventType)
	}

	return nil
}

func (r *OutboxRelay) Close() {
	r.kafkaClient.Close()
}
