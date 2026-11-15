package dev.kostas.trackstore.picture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

/**
 * Keeps the picture's liveness up to date: marks tracks without recent observations as
 * {@link TrackStatus#STALE} and removes tracks that have been silent for too long.
 * <p>
 * Runs every {@code check-interval}, the first time right after startup ({@code fixedDelay} with no
 * initial delay), so tracks that expired while the service was down are handled immediately.
 * <p>
 * Each step is one SQL statement whose condition is evaluated on the row at the moment it is
 * written. If the listener refreshes a track concurrently, the condition no longer holds and the
 * track is left alone — no read-then-write race.
 */
@Component
class PictureExpiryJob {

    private static final Logger log = LoggerFactory.getLogger(PictureExpiryJob.class);

    private final TrackStateRepository picture;
    private final PictureExpiryProperties expiry;
    private final Clock clock;

    PictureExpiryJob(TrackStateRepository picture, PictureExpiryProperties expiry, Clock clock) {
        this.picture = picture;
        this.expiry = expiry;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${trackstore.picture.check-interval}")
    @Transactional
    public void run() {
        Instant now = clock.instant();

        // Drop first: a track past both thresholds (e.g. after downtime) is just dropped.
        List<String> dropped = picture.deleteObservedBefore(expiry.dropCutoff(now));
        List<String> stale = picture.markStaleIfObservedBefore(expiry.staleCutoff(now));

        if (!dropped.isEmpty()) {
            log.info("Dropped {} track(s) with no observation for {}: {}", dropped.size(), expiry.dropAfter(), dropped);
        }
        if (!stale.isEmpty()) {
            log.info("{} track(s) went STALE, no observation for {}: {}", stale.size(), expiry.staleAfter(), stale);
        }
    }
}
