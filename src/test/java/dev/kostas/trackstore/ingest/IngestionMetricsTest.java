package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.track.TrackUpdate;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/** Meter names, tags and values, with an in-memory registry and a fixed clock. */
class IngestionMetricsTest {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private final IngestionMetrics metrics = new IngestionMetrics(registry, Clock.fixed(NOW, ZoneOffset.UTC));

    @Test
    void everySeriesExistsFromStartAtZero() {
        assertThat(registry.find("trackstore.updates").counters())
                .extracting(c -> c.getId().getTag("outcome"))
                .containsExactlyInAnyOrder("applied", "stale", "duplicate");
        assertThat(registry.find("trackstore.updates.rejected").counters())
                .extracting(c -> c.getId().getTag("reason"))
                .containsExactlyInAnyOrder("invalid_fields", "unparseable", "other");
        assertThat(registry.find("trackstore.updates").counters()).allSatisfy(c -> assertThat(c.count()).isZero());
    }

    @Test
    void countsEachOutcome() {
        metrics.processed(observedAt(NOW), ProcessingOutcome.APPLIED);
        metrics.processed(observedAt(NOW), ProcessingOutcome.DUPLICATE);
        metrics.processed(observedAt(NOW), ProcessingOutcome.DUPLICATE);

        assertThat(updates("applied")).isEqualTo(1);
        assertThat(updates("duplicate")).isEqualTo(2);
        assertThat(updates("stale")).isZero();
    }

    @Test
    void ageIsRecordedOnlyForUpdatesThatChangedThePicture() {
        metrics.processed(observedAt(NOW.minusMillis(1500)), ProcessingOutcome.APPLIED);
        metrics.processed(observedAt(NOW.minusSeconds(20)), ProcessingOutcome.STALE);       // not on the picture
        metrics.processed(observedAt(NOW.minusSeconds(20)), ProcessingOutcome.DUPLICATE);   // a replay, not fresh data

        Timer age = registry.get("trackstore.updates.age").timer();
        assertThat(age.count()).isEqualTo(1);
        assertThat(age.totalTime(TimeUnit.MILLISECONDS)).isEqualTo(1500);
    }

    @Test
    void timestampInTheFutureCountsAsZeroAge() {
        metrics.processed(observedAt(NOW.plusSeconds(2)), ProcessingOutcome.APPLIED);

        Timer age = registry.get("trackstore.updates.age").timer();
        assertThat(age.count()).isEqualTo(1);
        assertThat(age.totalTime(TimeUnit.MILLISECONDS)).isZero();
    }

    @Test
    void rejectionsAreCountedByCategoryEvenWhenWrappedBySpringKafka() {
        metrics.rejected(new ListenerExecutionFailedException("listener failed",
                new MessageConversionException("Cannot deserialize value of type `Identity`")));
        metrics.rejected(new IllegalStateException("something else"));

        assertThat(rejected("unparseable")).isEqualTo(1);
        assertThat(rejected("other")).isEqualTo(1);
        assertThat(rejected("invalid_fields")).isZero();   // covered end to end in TrackUpdateListenerIntegrationTest
    }

    private double updates(String outcome) {
        return registry.get("trackstore.updates").tag("outcome", outcome).counter().count();
    }

    private double rejected(String reason) {
        return registry.get("trackstore.updates.rejected").tag("reason", reason).counter().count();
    }

    private static TrackUpdate observedAt(Instant timestamp) {
        return aTrackUpdate().timestamp(timestamp).build();
    }
}
