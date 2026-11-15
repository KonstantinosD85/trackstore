package dev.kostas.trackstore.history;

import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.track.Identity;
import dev.kostas.trackstore.track.TrackUpdate;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;
import java.util.List;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs the repository against a real PostgreSQL (Testcontainers), with the Flyway schema applied.
 * {@link DataJpaTest} wraps each test in a transaction that is rolled back afterwards,
 * so every test starts with an empty table.
 */
@DataJpaTest
@Import(TestcontainersConfiguration.class)
class TrackHistoryRepositoryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Autowired
    private TrackHistoryRepository repository;

    @Nested
    class AppendIfAbsent {

        @Test
        void storesNewUpdateWithAllFields() {
            TrackUpdate update = aTrackUpdate()
                    .trackId("TRK-1").timestamp(T0)
                    .latitude(37.5).longitude(23.5).altitude(9_000.0).heading(270.0)
                    .identity(Identity.HOSTILE)
                    .build();

            boolean inserted = repository.appendIfAbsent(update);

            assertThat(inserted).isTrue();
            assertThat(repository.findAll()).singleElement().satisfies(entry -> {
                assertThat(entry.getTrackId()).isEqualTo("TRK-1");
                assertThat(entry.getObservedAt()).isEqualTo(T0);
                assertThat(entry.getLatitude()).isEqualTo(37.5);
                assertThat(entry.getLongitude()).isEqualTo(23.5);
                assertThat(entry.getAltitude()).isEqualTo(9_000.0);
                assertThat(entry.getHeading()).isEqualTo(270.0);
                assertThat(entry.getIdentity()).isEqualTo(Identity.HOSTILE);
                assertThat(entry.getReceivedAt()).isNotNull();
            });
        }

        @Test
        void storesMissingOptionalFieldsAsNull() {
            boolean inserted = repository.appendIfAbsent(aTrackUpdate().altitude(null).heading(null).build());

            assertThat(inserted).isTrue();
            assertThat(repository.findAll()).singleElement().satisfies(entry -> {
                assertThat(entry.getAltitude()).isNull();
                assertThat(entry.getHeading()).isNull();
            });
        }

        @Test
        void ignoresDuplicateOfSameTrackAndTimestamp() {
            TrackUpdate update = aTrackUpdate().trackId("TRK-1").timestamp(T0).build();

            boolean first = repository.appendIfAbsent(update);
            boolean redelivered = repository.appendIfAbsent(update);

            assertThat(first).isTrue();
            assertThat(redelivered).isFalse();
            assertThat(repository.count()).isEqualTo(1);
        }

        @Test
        void keepsFirstVersionWhenSameObservationArrivesWithDifferentContent() {
            repository.appendIfAbsent(aTrackUpdate().trackId("TRK-1").timestamp(T0).latitude(10.0).build());

            boolean inserted = repository.appendIfAbsent(
                    aTrackUpdate().trackId("TRK-1").timestamp(T0).latitude(20.0).build());

            assertThat(inserted).isFalse();
            assertThat(repository.findAll()).singleElement()
                    .extracting(TrackHistoryEntry::getLatitude).isEqualTo(10.0);
        }

        @Test
        void storesDifferentTimestampsOfSameTrack() {
            repository.appendIfAbsent(aTrackUpdate().trackId("TRK-1").timestamp(T0).build());
            repository.appendIfAbsent(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(1)).build());

            assertThat(repository.count()).isEqualTo(2);
        }

        @Test
        void storesSameTimestampOfDifferentTracks() {
            repository.appendIfAbsent(aTrackUpdate().trackId("TRK-1").timestamp(T0).build());
            repository.appendIfAbsent(aTrackUpdate().trackId("TRK-2").timestamp(T0).build());

            assertThat(repository.count()).isEqualTo(2);
        }
    }

    @Nested
    class FindHistory {

        @Test
        void returnsTrackUpdatesInChronologicalOrderRegardlessOfArrivalOrder() {
            // Arrival order: t+2, t0, t+1 (out of order)
            appendAt("TRK-1", T0.plusSeconds(2));
            appendAt("TRK-1", T0);
            appendAt("TRK-1", T0.plusSeconds(1));

            List<TrackHistoryEntry> history = repository.findHistory("TRK-1", Instant.EPOCH, T0.plusSeconds(60));

            assertThat(history).extracting(TrackHistoryEntry::getObservedAt)
                    .containsExactly(T0, T0.plusSeconds(1), T0.plusSeconds(2));
        }

        @Test
        void includesFromAndExcludesTo() {
            appendAt("TRK-1", T0.minusSeconds(1));
            appendAt("TRK-1", T0);                  // == from  → included
            appendAt("TRK-1", T0.plusSeconds(5));
            appendAt("TRK-1", T0.plusSeconds(10));  // == to    → excluded

            List<TrackHistoryEntry> history = repository.findHistory("TRK-1", T0, T0.plusSeconds(10));

            assertThat(history).extracting(TrackHistoryEntry::getObservedAt)
                    .containsExactly(T0, T0.plusSeconds(5));
        }

        @Test
        void returnsOnlyTheRequestedTrack() {
            appendAt("TRK-1", T0);
            appendAt("TRK-2", T0);

            List<TrackHistoryEntry> history = repository.findHistory("TRK-1", Instant.EPOCH, T0.plusSeconds(60));

            assertThat(history).extracting(TrackHistoryEntry::getTrackId).containsExactly("TRK-1");
        }

        @Test
        void returnsEmptyListForUnknownTrack() {
            assertThat(repository.findHistory("NOPE", Instant.EPOCH, T0)).isEmpty();
        }

        @Test
        void missingBoundsMeanUnbounded() {
            appendAt("TRK-1", T0.minusSeconds(10));
            appendAt("TRK-1", T0);
            appendAt("TRK-1", T0.plusSeconds(10));

            assertThat(repository.findHistory("TRK-1", null, null)).hasSize(3);
            assertThat(repository.findHistory("TRK-1", T0, null)).extracting(TrackHistoryEntry::getObservedAt)
                    .containsExactly(T0, T0.plusSeconds(10));
            assertThat(repository.findHistory("TRK-1", null, T0)).extracting(TrackHistoryEntry::getObservedAt)
                    .containsExactly(T0.minusSeconds(10));
        }

        private void appendAt(String trackId, Instant timestamp) {
            repository.appendIfAbsent(aTrackUpdate().trackId(trackId).timestamp(timestamp).build());
        }
    }

    @Test
    void existsByTrackIdTellsKnownFromUnknownTracks() {
        repository.appendIfAbsent(aTrackUpdate().trackId("TRK-1").build());

        assertThat(repository.existsByTrackId("TRK-1")).isTrue();
        assertThat(repository.existsByTrackId("NOPE")).isFalse();
    }
}
