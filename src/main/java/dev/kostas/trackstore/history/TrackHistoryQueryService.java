package dev.kostas.trackstore.history;

import dev.kostas.trackstore.track.TrackNotFoundException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Read side of the history. One read-only transaction covers both queries of a request.
 */
@Service
@Transactional(readOnly = true)
public class TrackHistoryQueryService {

    private final TrackHistoryRepository history;

    public TrackHistoryQueryService(TrackHistoryRepository history) {
        this.history = history;
    }

    /**
     * The track's accepted updates in chronological order, optionally limited to {@code [from, to)}
     * (either bound may be {@code null}). An empty list means "known track, nothing in the range";
     * an unknown track is a {@link TrackNotFoundException}.
     */
    public List<TrackHistoryEntry> history(String trackId, Instant from, Instant to) {
        List<TrackHistoryEntry> entries = history.findHistory(trackId, from, to);

        // Only an empty result needs the extra query to tell "unknown track" from "empty range".
        if (entries.isEmpty() && !history.existsByTrackId(trackId)) {
            throw new TrackNotFoundException(trackId);
        }
        return entries;
    }
}
