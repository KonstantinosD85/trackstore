package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.track.TrackUpdate;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;

/**
 * Ingestion metrics, in one place. Callers say what happened ({@link #processed}, {@link #rejected});
 * meter names and tags stay in here. {@link TrackUpdateService} doesn't know about metrics at all:
 * the listener and the error handler, at the edge, report to this class.
 * <p>
 * In Prometheus ({@code /prometheus}):
 * <ul>
 *   <li>{@code trackstore_updates_total{outcome}}: valid updates by {@link ProcessingOutcome}.
 *       A rising {@code duplicate} rate means redeliveries (crash, rebalance, replay); a rising
 *       {@code stale} rate means the feed arrives out of order.</li>
 *   <li>{@code trackstore_updates_rejected_total{reason}}: records sent to the dead-letter topic,
 *       by {@link RejectionReason.Category}.</li>
 *   <li>{@code trackstore_updates_age_seconds}: histogram of how old an update is when it reaches
 *       the picture (now − observation timestamp): sensor → producer → Kafka → us.</li>
 * </ul>
 * All counters are registered up front with every tag value, so each series exists from startup
 * at 0. Otherwise {@code rate()} has nothing to compare against until the first event.
 */
@Component
class IngestionMetrics {

    private final Map<ProcessingOutcome, Counter> processed = new EnumMap<>(ProcessingOutcome.class);
    private final Map<RejectionReason.Category, Counter> rejected = new EnumMap<>(RejectionReason.Category.class);
    private final Timer age;
    private final Clock clock;

    IngestionMetrics(MeterRegistry registry, Clock clock) {
        this.clock = clock;

        for (ProcessingOutcome outcome : ProcessingOutcome.values()) {
            processed.put(outcome, Counter.builder("trackstore.updates")
                    .description("Valid track updates processed, by outcome")
                    .tag("outcome", outcome.name().toLowerCase(java.util.Locale.ROOT))
                    .register(registry));
        }
        for (RejectionReason.Category reason : RejectionReason.Category.values()) {
            rejected.put(reason, Counter.builder("trackstore.updates.rejected")
                    .description("Track updates rejected and sent to the dead-letter topic, by reason")
                    .tag("reason", reason.tag())
                    .register(registry));
        }
        age = Timer.builder("trackstore.updates.age")
                .description("Age of an update when applied to the picture (now minus observation timestamp)")
                // Buckets, so Prometheus can compute percentiles across instances (histogram_quantile).
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(1))
                // An applied update is at most drop-after old (older ones stay out of the picture).
                .maximumExpectedValue(Duration.ofMinutes(1))
                .register(registry);
    }

    /** A valid update was processed. Its age is recorded only if it changed the picture. */
    void processed(TrackUpdate update, ProcessingOutcome outcome) {
        processed.get(outcome).increment();
        if (outcome == ProcessingOutcome.APPLIED) {
            Duration sinceObserved = Duration.between(update.timestamp(), clock.instant());
            // A timestamp slightly in the future (clocks out of sync) counts as "fresh", not negative.
            age.record(sinceObserved.isNegative() ? Duration.ZERO : sinceObserved);
        }
    }

    /** A record was given up on and sent to the dead-letter topic. */
    void rejected(Throwable exception) {
        rejected.get(RejectionReason.categorize(exception)).increment();
    }
}
