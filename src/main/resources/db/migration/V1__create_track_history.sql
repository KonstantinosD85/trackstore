-- Append-only log of every accepted track update.
CREATE TABLE track_history
(
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    track_id    VARCHAR(64)      NOT NULL,
    observed_at TIMESTAMPTZ      NOT NULL,          -- the update's own timestamp (event time)
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    altitude    DOUBLE PRECISION,
    heading     DOUBLE PRECISION,
    identity    VARCHAR(16)      NOT NULL,
    received_at TIMESTAMPTZ      NOT NULL DEFAULT now(), -- when we stored it (processing time)

    -- Idempotency: one observation per track per instant. Redelivered/replayed messages
    -- hit this constraint and are skipped (INSERT ... ON CONFLICT DO NOTHING).
    -- Its index also serves the history query: WHERE track_id = ? AND observed_at BETWEEN ...
    CONSTRAINT uq_track_history_track_observed UNIQUE (track_id, observed_at)
);
