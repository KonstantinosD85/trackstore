package dev.kostas.trackstore.ingest;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Health of track-update ingestion, shown as {@code consumerLag} in {@code /health}
 * (the bean name minus the {@code HealthIndicator} suffix), with the {@link ConsumerLag} per
 * partition in its details.
 * <p>
 * Status:
 * <ul>
 *   <li>{@code DOWN} if the broker can't be reached or a listener container is not running —
 *       then nothing is being ingested at all.</li>
 *   <li>{@code UP} otherwise, <em>even with high lag</em>. A lagging consumer is working through a
 *       backlog; restarting it (what orchestrators do with a DOWN instance) would only make the
 *       lag worse. Lag is reported in the details, for dashboards and alerts. Whether the instance
 *       should receive traffic yet is a separate question, answered by {@link CatchUpHealthIndicator}.</li>
 * </ul>
 */
@Component
class ConsumerLagHealthIndicator implements HealthIndicator {

    private final ConsumerLag lag;
    private final KafkaListenerEndpointRegistry listeners;

    ConsumerLagHealthIndicator(ConsumerLag lag, KafkaListenerEndpointRegistry listeners) {
        this.lag = lag;
        this.listeners = listeners;
    }

    @Override
    public Health health() {
        Map<Integer, Long> lagByPartition;
        try {
            lagByPartition = lag.byPartition();
        } catch (Exception e) {
            return Health.down(e).withDetail("consumerGroup", lag.groupId()).build();
        }

        boolean listenersRunning = !listeners.getListenerContainers().isEmpty()
                && listeners.getListenerContainers().stream().allMatch(MessageListenerContainer::isRunning);

        return (listenersRunning ? Health.up() : Health.down())
                .withDetail("topic", Topics.TRACK_UPDATES)
                .withDetail("consumerGroup", lag.groupId())
                .withDetail("listenersRunning", listenersRunning)
                .withDetail("totalLag", ConsumerLag.total(lagByPartition))
                .withDetail("lagByPartition", lagByPartition)
                .build();
    }
}
