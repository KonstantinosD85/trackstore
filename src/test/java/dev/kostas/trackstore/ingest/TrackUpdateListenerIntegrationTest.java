package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.KafkaTestcontainersConfiguration;
import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.history.TrackHistoryEntry;
import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.TrackState;
import dev.kostas.trackstore.picture.TrackStateRepository;
import dev.kostas.trackstore.track.Identity;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end test of ingestion: the full application with a real Kafka and PostgreSQL.
 * Raw JSON is published to {@value Topics#TRACK_UPDATES}, exactly like an external producer would.
 * <p>
 * The listener runs on its own threads and commits its own transactions, so nothing is rolled back
 * between tests. Each test therefore uses its own unique {@code trackId} and only looks at that.
 */
@SpringBootTest
@Import({TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class})
class TrackUpdateListenerIntegrationTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private TrackHistoryRepository history;

    @Autowired
    private TrackStateRepository picture;

    private final String trackId = "TRK-" + UUID.randomUUID();

    private Consumer<String, String> deadLetterConsumer;

    @BeforeEach
    void subscribeToDeadLetterTopic() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        deadLetterConsumer = consumerFactory.createConsumer("dlt-test-" + UUID.randomUUID(), null, null, properties);
        deadLetterConsumer.subscribe(List.of(Topics.TRACK_UPDATES_DLT));
    }

    @AfterEach
    void closeDeadLetterConsumer() {
        deadLetterConsumer.close();
    }

    @Test
    void validUpdateReachesPictureAndHistory() {
        publish(update(T0, 37.5, "HOSTILE"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            TrackState state = picture.findById(trackId).orElseThrow();
            assertThat(state.getObservedAt()).isEqualTo(T0);
            assertThat(state.getLatitude()).isEqualTo(37.5);
            assertThat(state.getIdentity()).isEqualTo(Identity.HOSTILE);
        });
        assertThat(historyOfTrack()).hasSize(1);
    }

    @Test
    void outOfOrderAndDuplicateUpdatesEndInLatestPictureAndCompleteHistory() {
        publish(update(T0, 10.0, "FRIEND"));
        publish(update(T0.plusSeconds(2), 12.0, "FRIEND"));
        publish(update(T0.plusSeconds(1), 11.0, "FRIEND"));   // out of order
        publish(update(T0.plusSeconds(2), 12.0, "FRIEND"));   // duplicate

        // Barrier: same key → same partition → processed after everything above.
        // Once it shows up in the DLT, all four updates have been processed.
        publish("not json");
        awaitDeadLetter();

        TrackState state = picture.findById(trackId).orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(T0.plusSeconds(2));
        assertThat(state.getLatitude()).isEqualTo(12.0);
        assertThat(historyOfTrack()).extracting(TrackHistoryEntry::getObservedAt)
                .containsExactly(T0, T0.plusSeconds(1), T0.plusSeconds(2));
    }

    @Test
    void invalidValuesAreDeadLetteredWithOriginalPayloadAndNeverStored() {
        String payload = update(T0, 91.0, "FRIEND");   // latitude out of range

        publish(payload);

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter();
        assertThat(deadLetter.value()).isEqualTo(payload);
        assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).endsWith("MethodArgumentNotValidException");
        assertNothingStored();
    }

    @Test
    void unknownIdentityIsDeadLetteredAsUnparseable() {
        String payload = update(T0, 37.5, "ENEMY");

        publish(payload);

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter();
        assertThat(deadLetter.value()).isEqualTo(payload);
        assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).endsWith("ConversionException");
        assertNothingStored();
    }

    @Test
    void invalidTimestampIsDeadLetteredAsUnparseable() {
        String payload = """
                {"trackId":"%s","timestamp":"yesterday","latitude":37.5,"longitude":23.5,"identity":"FRIEND"}
                """.formatted(trackId).strip();

        publish(payload);

        assertThat(awaitDeadLetter().value()).isEqualTo(payload);
        assertNothingStored();
    }

    @Test
    void malformedJsonIsDeadLetteredUnchanged() {
        publish("{ this is not json");

        assertThat(awaitDeadLetter().value()).isEqualTo("{ this is not json");
        assertNothingStored();
    }

    // --- helpers -------------------------------------------------------------------------------

    private String update(Instant timestamp, double latitude, String identity) {
        return """
                {"trackId":"%s","timestamp":"%s","latitude":%s,"longitude":23.5,"altitude":9000,"heading":90,"identity":"%s"}
                """.formatted(trackId, timestamp, latitude, identity).strip();
    }

    /** Publishes with this test's trackId as the key, like the real producer. */
    private void publish(String payload) {
        try {
            kafkaTemplate.send(Topics.TRACK_UPDATES, trackId, payload).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Could not publish test record", e);
        }
    }

    /** Waits for this test's record on the dead-letter topic; other tests' records are skipped. */
    private ConsumerRecord<String, String> awaitDeadLetter() {
        return await().atMost(TIMEOUT).until(
                () -> StreamSupport.stream(deadLetterConsumer.poll(Duration.ofMillis(500)).spliterator(), false)
                        .filter(record -> trackId.equals(record.key()))
                        .findFirst()
                        .orElse(null),
                record -> record != null);
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        assertThat(header).as("header %s", name).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private List<TrackHistoryEntry> historyOfTrack() {
        return history.findHistory(trackId, Instant.EPOCH, T0.plusSeconds(3600));
    }

    /** Safe right after {@link #awaitDeadLetter()}: the rejected record has been fully handled by then. */
    private void assertNothingStored() {
        assertThat(picture.findById(trackId)).isEmpty();
        assertThat(historyOfTrack()).isEmpty();
    }
}
