package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.track.TrackUpdate;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes {@value Topics#TRACK_UPDATES} and hands each update to {@link TrackUpdateService}.
 * <p>
 * Before this method runs, Spring Kafka:
 * <ol>
 *   <li>converts the JSON record value to {@link TrackUpdate} — failure throws a conversion
 *       exception (e.g. malformed JSON, {@code "identity": "ENEMY"});</li>
 *   <li>validates it because of {@link Valid @Valid} — failure throws
 *       {@code MethodArgumentNotValidException} (e.g. latitude 91).</li>
 * </ol>
 * Both are handled by the error handler in {@link KafkaConfig}: logged and sent to the
 * dead-letter topic, without retrying.
 * <p>
 * Ordering: records are keyed by {@code trackId}, so all updates of one track are on the
 * same partition, and each partition is consumed by one thread, in offset order.
 */
@Component
public class TrackUpdateListener {

    private static final Logger log = LoggerFactory.getLogger(TrackUpdateListener.class);

    private final TrackUpdateService service;
    private final IngestionMetrics metrics;

    // Package-private: only Spring creates the listener, and IngestionMetrics is internal to this package.
    TrackUpdateListener(TrackUpdateService service, IngestionMetrics metrics) {
        this.service = service;
        this.metrics = metrics;
    }

    @KafkaListener(topics = Topics.TRACK_UPDATES)
    public void onTrackUpdate(@Payload @Valid TrackUpdate update,
                              @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                              @Header(KafkaHeaders.OFFSET) long offset) {
        ProcessingOutcome outcome = service.process(update);
        // After the transaction committed. If process() throws, nothing is counted: the record is
        // retried, and counted once when it finally succeeds.
        metrics.processed(update, outcome);
        log.info("{} trackId={} timestamp={} [partition={}, offset={}]",
                outcome, update.trackId(), update.timestamp(), partition, offset);
    }
}
