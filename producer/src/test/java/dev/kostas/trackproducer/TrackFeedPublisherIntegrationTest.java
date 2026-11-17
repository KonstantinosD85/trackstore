package dev.kostas.trackproducer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The real producer configuration ({@link Main#createProducer}) against a real Kafka broker
 * (Testcontainers), read back with a plain consumer.
 */
class TrackFeedPublisherIntegrationTest {

    private static final String TOPIC = "track-updates";
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.0");   // same as compose.yaml

    @BeforeAll
    static void startKafkaAndCreateTopic() throws Exception {
        KAFKA.start();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 3, (short) 1))).all().get();
        }
    }

    @AfterAll
    static void stopKafka() {
        KAFKA.stop();
    }

    @Test
    void recordsReachKafkaAndAllUpdatesOfATrackLandOnOnePartition() throws Exception {
        DemoScenario scenario = new DemoScenario(new TrackSimulator(3), JsonMapper.builder().build(),
                Clock.systemUTC());

        try (Producer<String, String> producer = Main.createProducer(KAFKA.getBootstrapServers())) {
            new TrackFeedPublisher(producer, TOPIC).publish(scenario, DemoScenario.MIN_TICKS, Duration.ofMillis(10),
                    new CountDownLatch(1));
        }

        List<ConsumerRecord<String, String>> records = readAll(3 * 8 + 1 + 2);

        assertThat(records).hasSize(27);
        Map<String, Set<Integer>> partitionsByTrack = records.stream().collect(Collectors.groupingBy(
                ConsumerRecord::key, Collectors.mapping(ConsumerRecord::partition, Collectors.toSet())));
        // The key decides the partition, so each track's updates stay in order relative to each other.
        assertThat(partitionsByTrack).containsOnlyKeys("TRK-001", "TRK-002", "TRK-003")
                .allSatisfy((track, partitions) -> assertThat(partitions).hasSize(1));
    }

    private static List<ConsumerRecord<String, String>> readAll(int expected) {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of(TOPIC));
            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (records.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }
            return records;
        }
    }
}
