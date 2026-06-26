package domain

import "time"

type Player struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	Region    string    `json:"region,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}
