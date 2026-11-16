package dev.kostas.trackstore.ingest;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The lag arithmetic, without Kafka. */
class ConsumerLagTest {

    private static final TopicPartition P0 = new TopicPartition(Topics.TRACK_UPDATES, 0);
    private static final TopicPartition P1 = new TopicPartition(Topics.TRACK_UPDATES, 1);
    private static final TopicPartition P2 = new TopicPartition(Topics.TRACK_UPDATES, 2);

    @Test
    void lagIsLatestMinusCommittedPerPartition() {
        Map<Integer, Long> lag = ConsumerLag.compute(
                List.of(P0, P1),
                Map.of(P0, 0L, P1, 0L),
                Map.of(P0, 100L, P1, 50L),
                Map.of(P0, new OffsetAndMetadata(90), P1, new OffsetAndMetadata(50)));

        assertThat(lag).containsExactly(Map.entry(0, 10L), Map.entry(1, 0L));
    }

    @Test
    void partitionNeverCommittedCountsFromEarliestOffset() {
        // Earliest 20: records before it were deleted by retention, so they can't be consumed.
        Map<Integer, Long> lag = ConsumerLag.compute(
                List.of(P2), Map.of(P2, 20L), Map.of(P2, 25L), Map.of());

        assertThat(lag).containsExactly(Map.entry(2, 5L));
    }

    @Test
    void lagIsNeverNegative() {
        // Can briefly happen when the two offsets are read at slightly different moments.
        Map<Integer, Long> lag = ConsumerLag.compute(
                List.of(P0), Map.of(P0, 0L), Map.of(P0, 10L), Map.of(P0, new OffsetAndMetadata(11)));

        assertThat(lag).containsExactly(Map.entry(0, 0L));
    }
}
