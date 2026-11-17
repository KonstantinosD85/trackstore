package dev.kostas.trackstore.ingest;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/** The lag gauge: one series per partition, refreshed on schedule, removed when unknown. */
@ExtendWith(MockitoExtension.class)
class ConsumerLagMetricsTest {

    @Mock
    private ConsumerLag lag;

    private final MeterRegistry registry = new SimpleMeterRegistry();
    private ConsumerLagMetrics metrics;

    @BeforeEach
    void setUp() {
        when(lag.groupId()).thenReturn("trackstore");
        metrics = new ConsumerLagMetrics(lag, registry);
    }

    @Test
    void oneSeriesPerPartitionWithTheLatestValue() throws Exception {
        when(lag.byPartition()).thenReturn(Map.of(0, 120L, 1, 0L, 2, 7L), Map.of(0, 3L, 1, 0L, 2, 0L));

        metrics.refresh();
        assertThat(lagOf("0")).isEqualTo(120);
        assertThat(lagOf("2")).isEqualTo(7);

        metrics.refresh();
        assertThat(lagOf("0")).isEqualTo(3);
        assertThat(registry.get("trackstore.ingestion.lag").tag("consumer_group", "trackstore").gauges()).hasSize(3);
    }

    @Test
    void unknownLagRemovesTheSeriesInsteadOfFreezingTheLastValue() throws Exception {
        when(lag.byPartition())
                .thenReturn(Map.of(0, 120L))
                .thenThrow(new TimeoutException("broker unreachable"));

        metrics.refresh();
        assertThat(registry.find("trackstore.ingestion.lag").gauges()).hasSize(1);

        metrics.refresh();
        assertThat(registry.find("trackstore.ingestion.lag").gauges()).isEmpty();
    }

    private double lagOf(String partition) {
        Gauge gauge = registry.get("trackstore.ingestion.lag").tag("partition", partition).gauge();
        return gauge.value();
    }
}
