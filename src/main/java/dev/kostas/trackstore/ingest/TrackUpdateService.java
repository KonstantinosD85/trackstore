package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.TrackStateRepository;
import dev.kostas.trackstore.track.TrackUpdate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies incoming track updates to history and the picture.
 * <p>
 * Expects an already <em>validated</em> update: validation happens at the Kafka listener
 * ({@code @Payload @Valid}), which is the only entry point into this service.
 */
@Service
public class TrackUpdateService {

    private static final Logger log = LoggerFactory.getLogger(TrackUpdateService.class);

    private final TrackHistoryRepository history;
    private final TrackStateRepository picture;

    public TrackUpdateService(TrackHistoryRepository history, TrackStateRepository picture) {
        this.history = history;
        this.picture = picture;
    }

    /**
     * Processes one update. History and picture are written in a single transaction, so an
     * update is either fully applied or not at all. That makes history the duplicate check:
     * if the update is already in history, it was already applied to the picture too.
     */
    @Transactional
    public ProcessingOutcome process(TrackUpdate update) {
        if (!history.appendIfAbsent(update)) {
            log.debug("Duplicate update ignored: trackId={}, timestamp={}", update.trackId(), update.timestamp());
            return ProcessingOutcome.DUPLICATE;
        }

        if (!picture.upsertIfNewer(update)) {
            log.debug("Stale update kept in history only: trackId={}, timestamp={}", update.trackId(), update.timestamp());
            return ProcessingOutcome.STALE;
        }

        return ProcessingOutcome.APPLIED;
    }
}
