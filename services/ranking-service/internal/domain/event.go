package domain

import "time"

type ScoreSubmittedEvent struct {
	EventID   string    `json:"event_id"`
	PlayerID  string    `json:"player_id"`
	GameID    string    `json:"game_id"`
	Score     float64   `json:"score"`
	Timestamp time.Time `json:"timestamp"`
}
