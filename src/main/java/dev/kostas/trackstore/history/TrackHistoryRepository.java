package dev.kostas.trackstore.history;

import dev.kostas.trackstore.track.TrackUpdate;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.time.Instant;
import java.util.List;

public interface TrackHistoryRepository extends JpaRepository<TrackHistoryEntry, Long> {

    /**
     * Appends the update to history unless the same observation ({@code trackId} + {@code timestamp})
     * is already stored.
     *
     * @return {@code true} if a new row was inserted, {@code false} if it was a duplicate
     */
    default boolean appendIfAbsent(TrackUpdate update) {
        int inserted = insertIgnoringDuplicates(
                update.trackId(),
                update.timestamp(),
                update.latitude(),
                update.longitude(),
                update.altitude(),
                update.heading(),
                update.identity().name());
        return inserted == 1;
    }

    /**
     * Native insert that relies on the {@code (track_id, observed_at)} unique constraint.
     * Unlike {@code save()} + catching the constraint violation, {@code ON CONFLICT DO NOTHING}
     * never throws, so the surrounding transaction is not marked rollback-only.
     *
     * @return number of inserted rows: 1 for a new observation, 0 for a duplicate
     */
    @Modifying
    @Query(nativeQuery = true, value = """
            INSERT INTO track_history (track_id, observed_at, latitude, longitude, altitude, heading, identity)
            VALUES (:trackId, :observedAt, :latitude, :longitude, :altitude, :heading, :identity)
            ON CONFLICT (track_id, observed_at) DO NOTHING
            """)
    int insertIgnoringDuplicates(String trackId,
                                 Instant observedAt,
                                 Double latitude,
                                 Double longitude,
                                 Double altitude,
                                 Double heading,
                                 String identity);

    /** Stand-ins for "no bound"; both lie within PostgreSQL's TIMESTAMPTZ range. */
    Instant EARLIEST = Instant.parse("0001-01-01T00:00:00Z");
    Instant LATEST = Instant.parse("9999-12-31T23:59:59Z");

    /**
     * History of one track in chronological order, within {@code [from, to)}.
     * Both bounds are optional: {@code null} means "no lower bound" / "no upper bound".
     * <p>
     * The defaults are resolved here rather than with {@code :from is null or ...} in the query:
     * PostgreSQL cannot infer the type of a parameter that is only compared with null, and a plain
     * range keeps the query simple for the {@code (track_id, observed_at)} index.
     */
    default List<TrackHistoryEntry> findHistory(String trackId, Instant from, Instant to) {
        return findHistoryBetween(trackId, from != null ? from : EARLIEST, to != null ? to : LATEST);
    }

    @Query("""
            select h from TrackHistoryEntry h
            where h.trackId = :trackId
              and h.observedAt >= :from
              and h.observedAt < :to
            order by h.observedAt
            """)
    List<TrackHistoryEntry> findHistoryBetween(String trackId, Instant from, Instant to);

    /** Derived query: Spring Data generates it from the method name. */
    boolean existsByTrackId(String trackId);
}
