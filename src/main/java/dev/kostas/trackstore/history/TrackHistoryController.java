package dev.kostas.trackstore.history;

import dev.kostas.trackstore.track.TrackNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.List;

/**
 * Read API for the history of one track.
 */
@RestController
public class TrackHistoryController {

    private final TrackHistoryRepository history;

    public TrackHistoryController(TrackHistoryRepository history) {
        this.history = history;
    }

    /**
     * The track's accepted updates in chronological order (by observation time), optionally
     * limited to {@code [from, to)}: {@code from} inclusive, {@code to} exclusive, both optional
     * ISO-8601 instants such as {@code 2026-01-01T12:00:00Z}.
     * <p>
     * Unknown track → {@code 404}; known track with nothing in the range → {@code 200} with {@code []}.
     */
    @GetMapping("/tracks/{trackId}/history")
    public List<TrackHistoryEntryResponse> history(@PathVariable String trackId,
                                                   @RequestParam(required = false) Instant from,
                                                   @RequestParam(required = false) Instant to) {
        if (from != null && to != null && from.isAfter(to)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "'from' must not be after 'to'");
        }

        List<TrackHistoryEntry> entries = history.findHistory(trackId, from, to);

        // Only an empty result needs the extra query to tell "unknown track" from "empty range".
        if (entries.isEmpty() && !history.existsByTrackId(trackId)) {
            throw new TrackNotFoundException(trackId);
        }
        return entries.stream().map(TrackHistoryEntryResponse::from).toList();
    }
}
