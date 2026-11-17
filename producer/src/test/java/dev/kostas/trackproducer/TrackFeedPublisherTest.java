package dev.kostas.trackproducer;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The publishing loop with Kafka's {@link MockProducer}: an in-memory producer from kafka-clients
 * that records what was sent — no broker needed.
 */
class TrackFeedPublisherTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");
    private static final Duration INTERVAL = Duration.ofMillis(10);

    private final MockProducer<String, String> producer =
            new MockProducer<>(true, null, new StringSerializer(), new StringSerializer());   // auto-acknowledge

    private final TrackFeedPublisher publisher = new TrackFeedPublisher(producer, "track-updates");

    @Test
    void publishesEveryMessageOfEveryTickToTheTopicKeyedByTrackId() throws Exception {
        int ticks = publisher.publish(scenario(), DemoScenario.MIN_TICKS, INTERVAL, new CountDownLatch(1));

        assertThat(ticks).isEqualTo(DemoScenario.MIN_TICKS);
        // 3 tracks × 8 ticks, + 1 duplicate, + 2 invalid (the out-of-order one replaces a regular send)
        assertThat(producer.history()).hasSize(3 * 8 + 1 + 2)
                .allSatisfy(record -> {
                    assertThat(record.topic()).isEqualTo("track-updates");
                    assertThat(record.key()).matches("TRK-00[1-3]");
                    assertThat(record.value()).contains("\"trackId\":\"" + record.key() + "\"");
                });
    }

    @Test
    void stopsBetweenTicksWhenAskedToStop() throws Exception {
        CountDownLatch stop = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            // Runs "forever" (ticks = 0) until stopped.
            var run = executor.submit(() -> publisher.publish(scenario(), 0, INTERVAL, stop));

            Thread.sleep(100);
            stop.countDown();

            int ticks = run.get(5, TimeUnit.SECONDS);
            assertThat(ticks).isPositive();
            assertThat(producer.history()).extracting(ProducerRecord::key).isNotEmpty();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void publishesNothingWhenStoppedBeforeStart() throws Exception {
        CountDownLatch stop = new CountDownLatch(1);
        stop.countDown();

        assertThat(publisher.publish(scenario(), 0, INTERVAL, stop)).isZero();
        assertThat(producer.history()).isEmpty();
    }

    private static DemoScenario scenario() {
        return new DemoScenario(new TrackSimulator(3), JsonMapper.builder().build(), new SteppingClock(T0, INTERVAL));
    }
}
