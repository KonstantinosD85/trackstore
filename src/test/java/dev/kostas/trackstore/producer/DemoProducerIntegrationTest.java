package dev.kostas.trackstore.producer;

import dev.kostas.trackstore.KafkaTestcontainersConfiguration;
import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.history.TrackHistoryEntry;
import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.TrackState;
import dev.kostas.trackstore.picture.TrackStateRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The whole demo, end to end: the {@code producer} profile publishes its scenario to a real Kafka
 * while the application starts, and the service consumes it. The result must be what the
 * requirements ask for, despite the duplicate, the out-of-order update and the invalid messages.
 * <p>
 * The profile gives this test its own Spring context — and so its own, empty containers.
 */
@SpringBootTest(properties = {
        "trackstore.demo-producer.ticks=" + DemoProducerIntegrationTest.TICKS,
        "trackstore.demo-producer.interval=50ms"})
@ActiveProfiles("producer")
@Import({TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class})
class DemoProducerIntegrationTest {

    static final int TICKS = 8;
    private static final int TRACKS = 3;   // from application-producer.yaml

    @Autowired
    private TrackStateRepository picture;

    @Autowired
    private TrackHistoryRepository history;

    @Test
    void pictureShowsLatestStateAndHistoryHasEveryValidUpdateOnce() {
        // The producer has published everything before the test starts; consumption is asynchronous.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(picture.findAll()).hasSize(TRACKS);
            for (TrackState state : picture.findAll()) {
                List<TrackHistoryEntry> entries = history.findHistory(state.getTrackId(), null, null);

                // duplicate stored once, late update stored, invalid messages not stored
                assertThat(entries).hasSize(TICKS);
                // the late update did not overwrite the picture
                assertThat(state.getObservedAt()).isEqualTo(entries.getLast().getObservedAt());
            }
        });
    }
}
