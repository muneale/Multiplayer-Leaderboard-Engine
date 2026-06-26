package domain

import "time"

// ScoreSubmittedEvent is the message published to the score-events Kafka topic.
// event_id is used by the Ranking Service for idempotent deduplication.
type ScoreSubmittedEvent struct {
	EventID   string    `json:"event_id"`
	PlayerID  string    `json:"player_id"`
	GameID    string    `json:"game_id"`
	Score     float64   `json:"score"`
	Timestamp time.Time `json:"timestamp"`
}
