-- Liveness of each track in the picture, maintained by PictureExpiryJob:
--   ACTIVE: updated recently; STALE: no update for a while. Dropped tracks are deleted.
-- Existing rows start as ACTIVE; the job's first run (at startup) corrects them.
ALTER TABLE track_picture
    ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE';

-- The job selects rows by observation time on every run.
CREATE INDEX idx_track_picture_observed_at ON track_picture (observed_at);
