CREATE TABLE IF NOT EXISTS outbox (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_type VARCHAR(64) NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    processed  BOOLEAN NOT NULL DEFAULT false,
    processed_at TIMESTAMP
);
