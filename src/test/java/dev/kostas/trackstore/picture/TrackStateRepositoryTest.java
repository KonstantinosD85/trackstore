package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.track.Identity;
import dev.kostas.trackstore.track.TrackUpdate;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;

import java.time.Instant;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies "newer timestamp wins" against a real PostgreSQL with the Flyway schema applied.
 */
@DataJpaTest
@Import(TestcontainersConfiguration.class)
class TrackStateRepositoryTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Autowired
    private TrackStateRepository repository;

    @Autowired
    private EntityManager entityManager;

    @Test
    void firstUpdateOfUnknownTrackBecomesItsState() {
        TrackUpdate update = aTrackUpdate()
                .trackId("TRK-1").timestamp(T0)
                .latitude(37.5).longitude(23.5).altitude(9_000.0).heading(270.0)
                .identity(Identity.HOSTILE)
                .build();

        boolean changed = repository.upsertIfNewer(update);

        assertThat(changed).isTrue();
        TrackState state = repository.findById("TRK-1").orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(T0);
        assertThat(state.getLatitude()).isEqualTo(37.5);
        assertThat(state.getLongitude()).isEqualTo(23.5);
        assertThat(state.getAltitude()).isEqualTo(9_000.0);
        assertThat(state.getHeading()).isEqualTo(270.0);
        assertThat(state.getIdentity()).isEqualTo(Identity.HOSTILE);
        assertThat(state.getLastReceivedAt()).isNotNull();
    }

    @Test
    void newerUpdateReplacesState() {
        repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).latitude(10.0).build());

        boolean changed = repository.upsertIfNewer(
                aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(5)).latitude(11.0)
                        .identity(Identity.FRIEND).build());

        assertThat(changed).isTrue();
        TrackState state = repository.findById("TRK-1").orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(T0.plusSeconds(5));
        assertThat(state.getLatitude()).isEqualTo(11.0);
        assertThat(state.getIdentity()).isEqualTo(Identity.FRIEND);
    }

    @Test
    void olderUpdateArrivingLateDoesNotOverwriteNewerState() {
        repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(5)).latitude(11.0).build());

        boolean changed = repository.upsertIfNewer(
                aTrackUpdate().trackId("TRK-1").timestamp(T0).latitude(10.0).build());

        assertThat(changed).isFalse();
        TrackState state = repository.findById("TRK-1").orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(T0.plusSeconds(5));
        assertThat(state.getLatitude()).isEqualTo(11.0);
    }

    @Test
    void duplicateUpdateLeavesStateUnchanged() {
        TrackUpdate update = aTrackUpdate().trackId("TRK-1").timestamp(T0).build();
        repository.upsertIfNewer(update);

        boolean changed = repository.upsertIfNewer(update);

        assertThat(changed).isFalse();
        assertThat(repository.count()).isEqualTo(1);
    }

    @Test
    void sameTimestampWithDifferentContentKeepsFirstVersion() {
        repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).latitude(10.0).build());

        boolean changed = repository.upsertIfNewer(
                aTrackUpdate().trackId("TRK-1").timestamp(T0).latitude(20.0).build());

        assertThat(changed).isFalse();
        assertThat(repository.findById("TRK-1").orElseThrow().getLatitude()).isEqualTo(10.0);
    }

    @Test
    void newerUpdateWithoutOptionalFieldsClearsThem() {
        repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).altitude(9_000.0).heading(90.0).build());

        repository.upsertIfNewer(
                aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(1)).altitude(null).heading(null).build());

        TrackState state = repository.findById("TRK-1").orElseThrow();
        assertThat(state.getAltitude()).isNull();
        assertThat(state.getHeading()).isNull();
    }

    @Test
    void outOfOrderSequenceEndsWithLatestState() {
        // Arrival order t+1, t+3, t0, t+2 — the picture must end at t+3 regardless.
        for (int second : new int[]{1, 3, 0, 2}) {
            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(second))
                    .latitude((double) second).build());
        }

        TrackState state = repository.findById("TRK-1").orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(T0.plusSeconds(3));
        assertThat(state.getLatitude()).isEqualTo(3.0);
    }

    @Test
    void tracksAreIndependent() {
        repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(10)).build());

        // An "older" timestamp on another track is still that track's first state.
        boolean changed = repository.upsertIfNewer(aTrackUpdate().trackId("TRK-2").timestamp(T0).build());

        assertThat(changed).isTrue();
        assertThat(repository.findAll()).extracting(TrackState::getTrackId)
                .containsExactlyInAnyOrder("TRK-1", "TRK-2");
    }

    @Nested
    class Expiry {

        @Test
        void newTrackStartsActive() {
            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).build());

            assertThat(reload("TRK-1").getStatus()).isEqualTo(TrackStatus.ACTIVE);
        }

        @Test
        void marksOnlyActiveTracksObservedBeforeCutoff() {
            repository.upsertIfNewer(aTrackUpdate().trackId("OLD").timestamp(T0).build());
            repository.upsertIfNewer(aTrackUpdate().trackId("RECENT").timestamp(T0.plusSeconds(20)).build());

            assertThat(repository.markStaleIfObservedBefore(T0.plusSeconds(10))).containsExactly("OLD");
            // Already stale: not reported again.
            assertThat(repository.markStaleIfObservedBefore(T0.plusSeconds(10))).isEmpty();

            assertThat(reload("OLD").getStatus()).isEqualTo(TrackStatus.STALE);
            assertThat(reload("RECENT").getStatus()).isEqualTo(TrackStatus.ACTIVE);
        }

        @Test
        void newerUpdateMakesStaleTrackActiveAgain() {
            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).build());
            repository.markStaleIfObservedBefore(T0.plusSeconds(10));

            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(15)).build());

            assertThat(reload("TRK-1").getStatus()).isEqualTo(TrackStatus.ACTIVE);
        }

        @Test
        void olderUpdateDoesNotReviveStaleTrack() {
            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(5)).build());
            repository.markStaleIfObservedBefore(T0.plusSeconds(10));

            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).build());   // out of order

            assertThat(reload("TRK-1").getStatus()).isEqualTo(TrackStatus.STALE);
        }

        @Test
        void deletesTracksObservedBeforeCutoff() {
            repository.upsertIfNewer(aTrackUpdate().trackId("OLD").timestamp(T0).build());
            repository.upsertIfNewer(aTrackUpdate().trackId("RECENT").timestamp(T0.plusSeconds(20)).build());

            assertThat(repository.deleteObservedBefore(T0.plusSeconds(10))).containsExactly("OLD");

            entityManager.clear();
            assertThat(repository.findAll()).extracting(TrackState::getTrackId).containsExactly("RECENT");
        }

        @Test
        void droppedTrackComesBackWithANewObservation() {
            repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0).build());
            repository.deleteObservedBefore(T0.plusSeconds(10));

            boolean changed = repository.upsertIfNewer(aTrackUpdate().trackId("TRK-1").timestamp(T0.plusSeconds(60)).build());

            assertThat(changed).isTrue();
            assertThat(reload("TRK-1").getStatus()).isEqualTo(TrackStatus.ACTIVE);
        }

        /** The native statements bypass the persistence context; read the row fresh from the database. */
        private TrackState reload(String trackId) {
            entityManager.clear();
            return repository.findById(trackId).orElseThrow();
        }
    }
}
