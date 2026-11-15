-- Current picture: exactly one row per track holding its latest accepted state.
CREATE TABLE track_picture
(
    track_id         VARCHAR(64) PRIMARY KEY,
    observed_at      TIMESTAMPTZ      NOT NULL,          -- event time of the update shown
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    altitude         DOUBLE PRECISION,
    heading          DOUBLE PRECISION,
    identity         VARCHAR(16)      NOT NULL,
    last_received_at TIMESTAMPTZ      NOT NULL DEFAULT now() -- processing time of that update
);
