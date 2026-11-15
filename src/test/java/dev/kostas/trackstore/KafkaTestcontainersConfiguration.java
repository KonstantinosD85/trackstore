package dev.kostas.trackstore;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;

/**
 * A throwaway Kafka broker for tests that start the Kafka listener, so they never touch the
 * developer's local Kafka. Kept separate from {@link TestcontainersConfiguration} so that
 * database-only tests ({@code @DataJpaTest}) don't pay for starting Kafka.
 */
@TestConfiguration(proxyBeanMethods = false)
public class KafkaTestcontainersConfiguration {

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        // Same image as compose.yaml.
        return new KafkaContainer("apache/kafka:4.1.0");
    }
}
