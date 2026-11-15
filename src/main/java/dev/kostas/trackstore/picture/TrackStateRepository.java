package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.track.TrackUpdate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;

public interface TrackStateRepository extends JpaRepository<TrackState, String> {

    /**
     * Makes the update the track's current state if the track is unknown, or if the update is
     * strictly newer than the stored state. Older updates (out of order) and updates with the
     * same timestamp (duplicates) leave the picture unchanged.
     *
     * @return {@code true} if the picture changed, {@code false} if the update was stale or a duplicate
     */
    default boolean upsertIfNewer(TrackUpdate update) {
        int changed = insertOrReplaceIfNewer(
                update.trackId(),
                update.timestamp(),
                update.latitude(),
                update.longitude(),
                update.altitude(),
                update.heading(),
                update.identity().name());
        return changed == 1;
    }

    /**
     * Atomic compare-and-set in a single statement: the timestamp check and the write happen
     * inside the database, so concurrent writers cannot interleave between "read" and "write"
     * and overwrite a newer state (no lost updates).
     * <p>
     * {@code clearAutomatically} evicts entities cached in the persistence context, because this
     * native statement bypasses it; otherwise a later {@code findById} in the same transaction
     * could return the stale pre-update object.
     *
     * @return 1 if a row was inserted or updated, 0 if the stored state is the same age or newer
     */
    @Modifying(clearAutomatically = true)
    @Query(nativeQuery = true, value = """
            INSERT INTO track_picture (track_id, observed_at, latitude, longitude, altitude, heading, identity)
            VALUES (:trackId, :observedAt, :latitude, :longitude, :altitude, :heading, :identity)
            ON CONFLICT (track_id) DO UPDATE
                SET observed_at      = EXCLUDED.observed_at,
                    latitude         = EXCLUDED.latitude,
                    longitude        = EXCLUDED.longitude,
                    altitude         = EXCLUDED.altitude,
                    heading          = EXCLUDED.heading,
                    identity         = EXCLUDED.identity,
                    last_received_at = now()
                WHERE track_picture.observed_at < EXCLUDED.observed_at
            """)
    int insertOrReplaceIfNewer(String trackId,
                               Instant observedAt,
                               Double latitude,
                               Double longitude,
                               Double altitude,
                               Double heading,
                               String identity);
}
