package dev.kostas.trackstore.ingest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Health of track-update ingestion, shown as {@code consumerLag} in {@code /health}
 * (the bean name minus the {@code HealthIndicator} suffix).
 * <p>
 * <b>Lag</b> = records published to {@value Topics#TRACK_UPDATES} that our consumer group has not
 * yet committed — how far the picture is behind the feed. It is computed like
 * {@code kafka-consumer-groups.sh --describe}: latest offset minus committed offset, per partition,
 * asked from the broker through the Kafka {@link Admin} client.
 * <p>
 * Status:
 * <ul>
 *   <li>{@code DOWN} if the broker can't be reached or a listener container is not running —
 *       then nothing is being ingested at all.</li>
 *   <li>{@code UP} otherwise, <em>even with high lag</em>. A lagging consumer is working through a
 *       backlog; restarting it (what orchestrators do with a DOWN instance) would only make the
 *       lag worse. Lag is reported in the details, for dashboards and alerts.</li>
 * </ul>
 */
@Component
class ConsumerLagHealthIndicator implements HealthIndicator {

    private static final long TIMEOUT_SECONDS = 3;

    private final Admin admin;
    private final KafkaListenerEndpointRegistry listeners;
    private final String groupId;

    ConsumerLagHealthIndicator(Admin admin,
                               KafkaListenerEndpointRegistry listeners,
                               @Value("${spring.kafka.consumer.group-id}") String groupId) {
        this.admin = admin;
        this.listeners = listeners;
        this.groupId = groupId;
    }

    @Override
    public Health health() {
        Map<Integer, Long> lagByPartition;
        try {
            lagByPartition = lagByPartition();
        } catch (Exception e) {
            return Health.down(e).withDetail("consumerGroup", groupId).build();
        }

        boolean listenersRunning = !listeners.getListenerContainers().isEmpty()
                && listeners.getListenerContainers().stream().allMatch(MessageListenerContainer::isRunning);

        return (listenersRunning ? Health.up() : Health.down())
                .withDetail("topic", Topics.TRACK_UPDATES)
                .withDetail("consumerGroup", groupId)
                .withDetail("listenersRunning", listenersRunning)
                .withDetail("totalLag", lagByPartition.values().stream().mapToLong(Long::longValue).sum())
                .withDetail("lagByPartition", lagByPartition)
                .build();
    }

    private Map<Integer, Long> lagByPartition() throws Exception {
        List<TopicPartition> partitions = admin.describeTopics(List.of(Topics.TRACK_UPDATES))
                .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(Topics.TRACK_UPDATES).partitions().stream()
                .map(info -> new TopicPartition(Topics.TRACK_UPDATES, info.partition()))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        return lag(partitions, offsets(partitions, OffsetSpec.earliest()), offsets(partitions, OffsetSpec.latest()),
                committed);
    }

    private Map<TopicPartition, Long> offsets(List<TopicPartition> partitions, OffsetSpec spec) throws Exception {
        Map<TopicPartition, ListOffsetsResultInfo> result = admin
                .listOffsets(partitions.stream().collect(Collectors.toMap(Function.identity(), p -> spec)))
                .all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return result.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().offset()));
    }

    /**
     * Lag per partition: latest offset minus committed offset. A partition the group has never
     * committed counts from its earliest offset, since {@code auto-offset-reset: earliest} will
     * make the consumer read all of it.
     */
    static Map<Integer, Long> lag(List<TopicPartition> partitions,
                                  Map<TopicPartition, Long> earliest,
                                  Map<TopicPartition, Long> latest,
                                  Map<TopicPartition, OffsetAndMetadata> committed) {
        Map<Integer, Long> lag = new TreeMap<>();
        for (TopicPartition partition : partitions) {
            OffsetAndMetadata commit = committed.get(partition);
            long position = commit != null ? commit.offset() : earliest.get(partition);
            lag.put(partition.partition(), Math.max(0, latest.get(partition) - position));
        }
        return lag;
    }
}
