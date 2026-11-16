package dev.kostas.trackstore.ingest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** The catch-up rule, with the lag source mocked. */
@ExtendWith(MockitoExtension.class)
class CatchUpHealthIndicatorTest {

    private static final long READY_MAX_LAG = 10;

    @Mock
    private ConsumerLag lag;

    @Test
    void notReadyWhileTheBacklogIsAboveTheThreshold() throws Exception {
        when(lag.byPartition()).thenReturn(Map.of(0, 500L, 1, 20L, 2, 0L));

        Health health = indicator().health();

        assertThat(health.getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
        assertThat(health.getDetails()).containsEntry("totalLag", 520L).containsEntry("caughtUp", false);
    }

    @Test
    void readyOnceTheLagIsAtMostTheThreshold() throws Exception {
        when(lag.byPartition()).thenReturn(Map.of(0, 4L, 1, 6L, 2, 0L));   // total 10

        assertThat(indicator().health().getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void staysReadyWhenLagRisesAgainLater() throws Exception {
        when(lag.byPartition()).thenReturn(Map.of(0, 0L), Map.of(0, 5_000L));
        CatchUpHealthIndicator indicator = indicator();

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);   // caught up
        assertThat(indicator.health().getStatus()).isEqualTo(Status.UP);   // burst: still ready
    }

    @Test
    void notReadyWhenTheLagCannotBeDetermined() throws Exception {
        when(lag.byPartition()).thenThrow(new TimeoutException("broker unreachable"));

        assertThat(indicator().health().getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    private CatchUpHealthIndicator indicator() {
        return new CatchUpHealthIndicator(lag, READY_MAX_LAG);
    }
}
