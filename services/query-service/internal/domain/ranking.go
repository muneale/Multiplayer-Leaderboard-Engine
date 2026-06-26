package domain

// RankedEntry is a single row in a leaderboard response.
// Rank is 1-based (top player = rank 1).
type RankedEntry struct {
	Rank     int64   `json:"rank"`
	PlayerID string  `json:"player_id"`
	Score    float64 `json:"score"`
}
