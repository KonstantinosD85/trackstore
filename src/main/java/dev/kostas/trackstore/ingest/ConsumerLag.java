package dev.kostas.trackstore.ingest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * How far our consumer group is behind {@value Topics#TRACK_UPDATES}: records published but not yet
 * committed — i.e. how far the picture is behind the feed.
 * <p>
 * Computed like {@code kafka-consumer-groups.sh --describe}: latest offset minus committed offset,
 * per partition, asked from the broker through the Kafka {@link Admin} client. It covers the
 * <em>whole group</em>, whichever instance owns which partition. Used by
 * {@link ConsumerLagHealthIndicator} and {@link CatchUpHealthIndicator}.
 */
@Component
class ConsumerLag {

    private static final long TIMEOUT_SECONDS = 3;

    private final Admin admin;
    private final String groupId;

    ConsumerLag(Admin admin, @Value("${spring.kafka.consumer.group-id}") String groupId) {
        this.admin = admin;
        this.groupId = groupId;
    }

    String groupId() {
        return groupId;
    }

    /**
     * @return lag per partition number, sorted by partition
     * @throws Exception if the broker can't be reached in time or the topic doesn't exist
     */
    Map<Integer, Long> byPartition() throws Exception {
        List<TopicPartition> partitions = admin.describeTopics(List.of(Topics.TRACK_UPDATES))
                .allTopicNames().get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .get(Topics.TRACK_UPDATES).partitions().stream()
                .map(info -> new TopicPartition(Topics.TRACK_UPDATES, info.partition()))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(groupId)
                .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        return compute(partitions, offsets(partitions, OffsetSpec.earliest()), offsets(partitions, OffsetSpec.latest()),
                committed);
    }

    static long total(Map<Integer, Long> lagByPartition) {
        return lagByPartition.values().stream().mapToLong(Long::longValue).sum();
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
    static Map<Integer, Long> compute(List<TopicPartition> partitions,
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
