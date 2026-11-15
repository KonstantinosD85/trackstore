package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.TestcontainersConfiguration;
import jakarta.persistence.EntityManager;
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

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * One run of the expiry job against a real PostgreSQL, with a fixed clock.
 * <p>
 * The JPA slice does not include {@code PictureConfig}, so {@code @EnableScheduling} is off here:
 * the job only runs when the test calls it.
 */
@DataJpaTest
@Import({TestcontainersConfiguration.class, PictureExpiryJob.class, PictureExpiryJobTest.FixedTime.class})
class PictureExpiryJobTest {

    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");

    @TestConfiguration(proxyBeanMethods = false)
    static class FixedTime {

        @Bean
        Clock clock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        PictureExpiryProperties pictureExpiryProperties() {
            return new PictureExpiryProperties(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(2));
        }
    }

    @Autowired
    private PictureExpiryJob job;

    @Autowired
    private TrackStateRepository picture;

    @Autowired
    private EntityManager entityManager;

    @Test
    void keepsRecentTracksMarksQuietOnesStaleAndDropsSilentOnes() {
        observe("RECENT", NOW.minusSeconds(5));
        observe("QUIET", NOW.minusSeconds(15));
        observe("SILENT", NOW.minusSeconds(31));

        job.run();

        entityManager.clear();
        assertThat(picture.findById("RECENT").orElseThrow().getStatus()).isEqualTo(TrackStatus.ACTIVE);
        assertThat(picture.findById("QUIET").orElseThrow().getStatus()).isEqualTo(TrackStatus.STALE);
        assertThat(picture.findById("SILENT")).isEmpty();
    }

    @Test
    void thresholdsAreExclusive() {
        observe("EXACTLY-STALE-AFTER", NOW.minusSeconds(10));
        observe("EXACTLY-DROP-AFTER", NOW.minusSeconds(30));

        job.run();

        entityManager.clear();
        assertThat(picture.findById("EXACTLY-STALE-AFTER").orElseThrow().getStatus()).isEqualTo(TrackStatus.ACTIVE);
        assertThat(picture.findById("EXACTLY-DROP-AFTER")).isPresent();
    }

    private void observe(String trackId, Instant timestamp) {
        picture.upsertIfNewer(aTrackUpdate().trackId(trackId).timestamp(timestamp).build());
    }
}
