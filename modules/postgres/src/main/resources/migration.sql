CREATE TABLE IF NOT EXISTS events (
    global_position BIGSERIAL PRIMARY KEY,
    tags            TEXT[]      NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         TEXT        NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_tags ON events USING GIN (tags);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
