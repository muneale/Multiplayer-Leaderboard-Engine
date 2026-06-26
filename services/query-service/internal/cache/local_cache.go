package cache

import (
	"sync"
	"time"
)

// Cache is the in-process store for leaderboard query results.
type Cache interface {
	Get(key string) []byte
	Set(key string, data []byte)
}

type entry struct {
	data      []byte
	expiresAt time.Time
}

// LocalCache is a thread-safe in-memory cache with a fixed TTL per entry.
// Stale entries are removed lazily on the next Get call for the same key.
type LocalCache struct {
	mu      sync.RWMutex
	entries map[string]entry
	ttl     time.Duration
}

func NewLocalCache(ttl time.Duration) *LocalCache {
	return &LocalCache{
		entries: make(map[string]entry),
		ttl:     ttl,
	}
}

func (c *LocalCache) Get(key string) []byte {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expiresAt) {
		return nil
	}
	return e.data
}

func (c *LocalCache) Set(key string, data []byte) {
	c.mu.Lock()
	c.entries[key] = entry{data: data, expiresAt: time.Now().Add(c.ttl)}
	c.mu.Unlock()
}
