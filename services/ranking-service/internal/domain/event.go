package domain

import "time"

type ScoreSubmittedEvent struct {
	EventID   string    `json:"event_id" avro:"event_id"`
	PlayerID  string    `json:"player_id" avro:"player_id"`
	GameID    string    `json:"game_id" avro:"game_id"`
	Score     float64   `json:"score" avro:"score"`
	Timestamp time.Time `json:"timestamp" avro:"timestamp"`
}
