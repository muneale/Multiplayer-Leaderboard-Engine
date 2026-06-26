package ranking

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"

	"ranking-service/internal/domain"
)

type EventProcessor struct {
	store        RankStore
	eventsTotal  metric.Int64Counter
	zaddDuration metric.Float64Histogram
}

func NewEventProcessor(store RankStore, meter metric.Meter) (*EventProcessor, error) {
	counter, err := meter.Int64Counter(
		"events_processed_total",
		metric.WithDescription("Total events processed partitioned by outcome status"),
	)
	if err != nil {
		return nil, err
	}
	histo, err := meter.Float64Histogram(
		"ranking_update_duration_seconds",
		metric.WithDescription("Time spent applying ZADD to Redis"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}
	return &EventProcessor{store: store, eventsTotal: counter, zaddDuration: histo}, nil
}

func (p *EventProcessor) Process(ctx context.Context, event *domain.ScoreSubmittedEvent) error {
	ctx, span := otel.Tracer("ranking-service").Start(ctx, "processor.Process")
	defer span.End()

	span.SetAttributes(
		attribute.String("event.id", event.EventID),
		attribute.String("player.id", event.PlayerID),
		attribute.String("game.id", event.GameID),
		attribute.Float64("score.value", event.Score),
	)

	isNew, err := p.store.IsNewEvent(ctx, event.EventID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.eventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error")))
		return err
	}
	if !isNew {
		span.SetAttributes(attribute.Bool("event.duplicate", true))
		p.eventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "duplicate")))
		return nil
	}

	start := time.Now()
	err = p.store.UpdateScore(ctx, event.GameID, event.PlayerID, event.Score)
	p.zaddDuration.Record(ctx, time.Since(start).Seconds())

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		p.eventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "error")))
		return err
	}

	span.SetStatus(codes.Ok, "")
	p.eventsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("status", "processed")))
	return nil
}
