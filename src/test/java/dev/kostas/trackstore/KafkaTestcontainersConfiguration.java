package dev.kostas.trackstore;

import dev.kostas.trackstore.ingest.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A throwaway Kafka broker for tests that start the Kafka listener, so they never touch the
 * developer's local Kafka. Kept separate from {@link TestcontainersConfiguration} so that
 * database-only tests ({@code @DataJpaTest}) don't pay for starting Kafka.
 * <p>
 * The service doesn't create topics (in development {@code kafka-init} in compose.yaml does), so
 * the tests provision the same topics here. Spring Kafka's {@code KafkaAdmin} creates
 * {@link NewTopic} beans at startup, before the listener subscribes.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestcontainersConfiguration {

    /** Same partition count as compose.yaml. */
    private static final int PARTITIONS = 3;

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        // Same image as compose.yaml.
        return new KafkaContainer("apache/kafka:4.1.0");
    }

    @Bean
    NewTopic trackUpdatesTopic() {
        return TopicBuilder.name(Topics.TRACK_UPDATES).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic trackUpdatesDeadLetterTopic() {
        return TopicBuilder.name(Topics.TRACK_UPDATES_DLT).partitions(PARTITIONS).replicas(1).build();
    }
}
