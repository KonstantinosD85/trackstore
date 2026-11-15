package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.PictureExpiryProperties;
import dev.kostas.trackstore.picture.TrackStateRepository;
import dev.kostas.trackstore.track.TrackUpdate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test of the service's decisions, with the repositories mocked.
 */
@ExtendWith(MockitoExtension.class)
class TrackUpdateServiceTest {

    @Mock
    private TrackHistoryRepository history;

    @Mock
    private TrackStateRepository picture;

    private TrackUpdateService service;

    /** "Now" for the service: 10 s after the fixtures' default timestamp. */
    private static final Instant NOW = aTrackUpdate().build().timestamp().plusSeconds(10);

    @BeforeEach
    void createService() {
        var expiry = new PictureExpiryProperties(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(2));
        service = new TrackUpdateService(history, picture, expiry, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    void newLatestUpdateIsApplied() {
        TrackUpdate update = aTrackUpdate().build();
        when(history.appendIfAbsent(update)).thenReturn(true);
        when(picture.upsertIfNewer(update)).thenReturn(true);

        assertThat(service.process(update)).isEqualTo(ProcessingOutcome.APPLIED);
    }

    @Test
    void newButOlderUpdateIsStale() {
        TrackUpdate update = aTrackUpdate().build();
        when(history.appendIfAbsent(update)).thenReturn(true);
        when(picture.upsertIfNewer(update)).thenReturn(false);

        assertThat(service.process(update)).isEqualTo(ProcessingOutcome.STALE);
    }

    @Test
    void duplicateIsDetectedByHistoryAndNeverReachesPicture() {
        TrackUpdate update = aTrackUpdate().build();
        when(history.appendIfAbsent(update)).thenReturn(false);

        assertThat(service.process(update)).isEqualTo(ProcessingOutcome.DUPLICATE);
        verify(picture, never()).upsertIfNewer(any(TrackUpdate.class));
    }

    @Test
    void updateAlreadyPastDropAfterGoesToHistoryOnly() {
        TrackUpdate update = aTrackUpdate().timestamp(NOW.minusSeconds(31)).build();
        when(history.appendIfAbsent(update)).thenReturn(true);

        assertThat(service.process(update)).isEqualTo(ProcessingOutcome.STALE);
        verify(history).appendIfAbsent(update);
        verify(picture, never()).upsertIfNewer(any(TrackUpdate.class));
    }
}
