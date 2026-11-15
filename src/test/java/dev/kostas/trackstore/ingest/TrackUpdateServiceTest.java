package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.TrackStateRepository;
import dev.kostas.trackstore.track.TrackUpdate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

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

    @InjectMocks
    private TrackUpdateService service;

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
}
