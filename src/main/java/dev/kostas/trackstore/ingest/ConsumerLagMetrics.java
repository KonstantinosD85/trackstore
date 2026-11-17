package dev.kostas.trackstore.ingest;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * Consumer lag as a metric, so it can be graphed and alerted on over time (on {@code /health} it is
 * only the current value): {@code trackstore_ingestion_lag_records{partition}} in Prometheus,
 * {@code sum(trackstore_ingestion_lag_records)} for the total.
 * <p>
 * The value is refreshed on a schedule, not when Prometheus scrapes: reading the lag means three
 * calls to the broker ({@link ConsumerLag}), which shouldn't happen on every scrape, and a scrape
 * shouldn't hang for seconds when the broker is down. A scrape only reads the last value.
 * <p>
 * If the lag can't be read, the series are removed instead of keeping an old value. In a graph that
 * shows as a gap ("unknown"), which is honest; a frozen last value would look like a healthy flat line.
 */
@Component
class ConsumerLagMetrics {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLagMetrics.class);

    private final ConsumerLag lag;
    private final MultiGauge gauge;

    ConsumerLagMetrics(ConsumerLag lag, MeterRegistry registry) {
        this.lag = lag;
        this.gauge = MultiGauge.builder("trackstore.ingestion.lag")
                .description("Records published to track-updates but not yet committed by the consumer group")
                .baseUnit("records")
                .tag("consumer_group", lag.groupId())
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${trackstore.ingestion.lag-metric-interval}")
    void refresh() {
        try {
            update(lag.byPartition());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            update(Map.of());
        } catch (Exception e) {
            log.debug("Consumer lag unavailable, removing the lag series: {}", e.toString());
            update(Map.of());
        }
    }

    private void update(Map<Integer, Long> lagByPartition) {
        List<MultiGauge.Row<?>> rows = lagByPartition.entrySet().stream()
                .<MultiGauge.Row<?>>map(entry -> MultiGauge.Row.of(
                        Tags.of("partition", String.valueOf(entry.getKey())), entry.getValue()))
                .toList();
        gauge.register(rows, true);   // overwrite: new values, and partitions that are gone disappear
    }
}
