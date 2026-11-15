package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.history.TrackHistoryEntry;
import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.PictureExpiryProperties;
import dev.kostas.trackstore.picture.TrackState;
import dev.kostas.trackstore.picture.TrackStateRepository;
import dev.kostas.trackstore.track.TrackUpdate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The service wired to the real repositories and a real PostgreSQL: checks that a realistic
 * stream — with a duplicate and an out-of-order update — ends in the right picture and history.
 * <p>
 * {@link DataJpaTest} only loads the JPA slice, so the service and its collaborators
 * (expiry settings, a fixed clock) are added explicitly.
 */
@DataJpaTest
@Import({TestcontainersConfiguration.class, TrackUpdateService.class, TrackUpdateServiceIntegrationTest.FixedTime.class})
class TrackUpdateServiceIntegrationTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @TestConfiguration(proxyBeanMethods = false)
    static class FixedTime {

        @Bean
        Clock clock() {
            return Clock.fixed(T0.plusSeconds(5), ZoneOffset.UTC);
        }

        @Bean
        PictureExpiryProperties pictureExpiryProperties() {
            return new PictureExpiryProperties(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(2));
        }
    }

    @Autowired
    private TrackUpdateService service;

    @Autowired
    private TrackHistoryRepository history;

    @Autowired
    private TrackStateRepository picture;

    @Test
    void mixedStreamEndsInLatestPictureAndCompleteHistory() {
        TrackUpdate t0 = at(T0, 10.0);
        TrackUpdate t1 = at(T0.plusSeconds(1), 11.0);
        TrackUpdate t2 = at(T0.plusSeconds(2), 12.0);

        // Arrival order: t0, t2, t1 (out of order), t2 again (duplicate)
        assertThat(service.process(t0)).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(service.process(t2)).isEqualTo(ProcessingOutcome.APPLIED);
        assertThat(service.process(t1)).isEqualTo(ProcessingOutcome.STALE);
        assertThat(service.process(t2)).isEqualTo(ProcessingOutcome.DUPLICATE);

        TrackState state = picture.findById("TRK-1").orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(T0.plusSeconds(2));
        assertThat(state.getLatitude()).isEqualTo(12.0);

        List<TrackHistoryEntry> entries = history.findHistory("TRK-1", Instant.EPOCH, T0.plusSeconds(60));
        assertThat(entries).extracting(TrackHistoryEntry::getObservedAt)
                .containsExactly(T0, T0.plusSeconds(1), T0.plusSeconds(2));
    }

    @Test
    void replayingTheWholeStreamChangesNothing() {
        List<TrackUpdate> stream = List.of(at(T0, 10.0), at(T0.plusSeconds(2), 12.0), at(T0.plusSeconds(1), 11.0));
        stream.forEach(service::process);

        List<ProcessingOutcome> replayOutcomes = stream.stream().map(service::process).toList();

        assertThat(replayOutcomes).containsOnly(ProcessingOutcome.DUPLICATE);
        assertThat(history.count()).isEqualTo(3);
        assertThat(picture.findById("TRK-1").orElseThrow().getObservedAt()).isEqualTo(T0.plusSeconds(2));
    }

    private static TrackUpdate at(Instant timestamp, double latitude) {
        return aTrackUpdate().trackId("TRK-1").timestamp(timestamp).latitude(latitude).build();
    }
}
