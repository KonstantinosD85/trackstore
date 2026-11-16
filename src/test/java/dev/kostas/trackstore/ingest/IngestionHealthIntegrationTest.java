package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.KafkaTestcontainersConfiguration;
import dev.kostas.trackstore.TestcontainersConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.assertj.MockMvcTester;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * {@code /health} and {@code /metrics} in the running application, with a real
 * Kafka. Same context as the controller tests (same configuration), so no extra containers.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import({TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class})
class IngestionHealthIntegrationTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private MockMvcTester mvc;

    @Autowired
    private KafkaListenerEndpointRegistry listeners;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void healthIsUpWithLagPerPartitionAndDatabase() {
        assertThat(mvc.get().uri("/health"))
                .hasStatusOk()
                .bodyJson().isLenientlyEqualTo("""
                        {
                          "status": "UP",
                          "components": {
                            "db": { "status": "UP" },
                            "consumerLag": {
                              "status": "UP",
                              "details": { "topic": "track-updates", "consumerGroup": "trackstore", "listenersRunning": true }
                            }
                          }
                        }
                        """);

        assertThat(mvc.get().uri("/health"))
                .bodyJson().extractingPath("$.components.consumerLag.details.lagByPartition")
                .asMap().containsOnlyKeys("0", "1", "2");
    }

    @Test
    void stoppedListenerIsDownWhileLagGrowsAndRecoversAfterRestart() {
        listeners.stop();
        try {
            String key = "LAG-" + UUID.randomUUID();
            for (int i = 0; i < 5; i++) {
                publish(key, "not json " + i);   // anything counts as lag; these end in the DLT
            }

            await().atMost(TIMEOUT).untilAsserted(() -> {
                assertThat(mvc.get().uri("/health")).hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                        .bodyJson().extractingPath("$.components.consumerLag.status").isEqualTo("DOWN");
                assertThat(mvc.get().uri("/health"))
                        .bodyJson().extractingPath("$.components.consumerLag.details.totalLag").asNumber()
                        .satisfies(lag -> assertThat(lag.longValue()).isGreaterThanOrEqualTo(5));
            });
        } finally {
            listeners.start();
        }

        // Back up, and the backlog is consumed and committed.
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(mvc.get().uri("/health")).hasStatusOk();
            assertThat(mvc.get().uri("/health"))
                    .bodyJson().extractingPath("$.components.consumerLag.details.totalLag").asNumber()
                    .satisfies(lag -> assertThat(lag.longValue()).isZero());
        });
    }

    @Test
    void kafkaClientLagMetricIsPublished() {
        // Spring Boot binds the Kafka consumer's own metrics to Micrometer automatically.
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(mvc.get().uri("/metrics/kafka.consumer.fetch.manager.records.lag.max"))
                        .hasStatusOk());
    }

    private void publish(String key, String payload) {
        try {
            kafkaTemplate.send(Topics.TRACK_UPDATES, key, payload).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish test record", e);
        }
    }
}
