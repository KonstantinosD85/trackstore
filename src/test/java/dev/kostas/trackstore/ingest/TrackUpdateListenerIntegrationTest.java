package dev.kostas.trackstore.ingest;

import dev.kostas.trackstore.KafkaTestcontainersConfiguration;
import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.history.TrackHistoryEntry;
import dev.kostas.trackstore.history.TrackHistoryRepository;
import dev.kostas.trackstore.picture.TrackState;
import dev.kostas.trackstore.picture.TrackStateRepository;
import dev.kostas.trackstore.picture.TrackStatus;
import dev.kostas.trackstore.track.Identity;
import io.micrometer.core.instrument.MeterRegistry;
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
import java.time.temporal.ChronoUnit;
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

    /**
     * Recent, because the full application runs on the real clock: an update older than
     * {@code drop-after} would (correctly) be kept out of the picture. Per test instance.
     */
    private final Instant t0 = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    @Autowired
    private TrackHistoryRepository history;

    @Autowired
    private TrackStateRepository picture;

    @Autowired
    private MeterRegistry meters;

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
        publish(update(t0, 37.5, "HOSTILE"));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            TrackState state = picture.findById(trackId).orElseThrow();
            assertThat(state.getObservedAt()).isEqualTo(t0);
            assertThat(state.getLatitude()).isEqualTo(37.5);
            assertThat(state.getIdentity()).isEqualTo(Identity.HOSTILE);
        });
        assertThat(historyOfTrack()).hasSize(1);
    }

    @Test
    void outOfOrderAndDuplicateUpdatesEndInLatestPictureAndCompleteHistory() {
        publish(update(t0, 10.0, "FRIEND"));
        publish(update(t0.plusSeconds(2), 12.0, "FRIEND"));
        publish(update(t0.plusSeconds(1), 11.0, "FRIEND"));   // out of order
        publish(update(t0.plusSeconds(2), 12.0, "FRIEND"));   // duplicate

        // Barrier: same key → same partition → processed after everything above.
        // Once it shows up in the DLT, all four updates have been processed.
        publish("not json");
        awaitDeadLetter();

        TrackState state = picture.findById(trackId).orElseThrow();
        assertThat(state.getObservedAt()).isEqualTo(t0.plusSeconds(2));
        assertThat(state.getLatitude()).isEqualTo(12.0);
        assertThat(historyOfTrack()).extracting(TrackHistoryEntry::getObservedAt)
                .containsExactly(t0, t0.plusSeconds(1), t0.plusSeconds(2));
    }

    @Test
    void metricsCountOutcomesRejectionsAndAge() {
        // Counters are shared by the whole test context, so compare before/after.
        double applied = updates("applied"), stale = updates("stale"), duplicate = updates("duplicate");
        double invalid = rejected("invalid_fields"), unparseable = rejected("unparseable");
        long ages = meters.get("trackstore.updates.age").timer().count();

        publish(update(t0, 10.0, "FRIEND"));                    // applied
        publish(update(t0.plusSeconds(2), 12.0, "FRIEND"));     // applied
        publish(update(t0.plusSeconds(1), 11.0, "FRIEND"));     // stale (out of order)
        publish(update(t0.plusSeconds(2), 12.0, "FRIEND"));     // duplicate
        publish(update(t0, 91.0, "FRIEND"));                    // rejected: invalid fields
        publish("not json");                                    // rejected: unparseable, and the barrier
        // Same key → same partition, in order: once the barrier is dead-lettered, all of the above are done.
        await().atMost(TIMEOUT).until(() -> StreamSupport
                .stream(deadLetterConsumer.poll(Duration.ofMillis(500)).spliterator(), false)
                .anyMatch(record -> trackId.equals(record.key()) && "not json".equals(record.value())));

        // At least: records of other tests may still be finishing in the shared context.
        assertThat(updates("applied") - applied).isGreaterThanOrEqualTo(2);
        assertThat(updates("stale") - stale).isGreaterThanOrEqualTo(1);
        assertThat(updates("duplicate") - duplicate).isGreaterThanOrEqualTo(1);
        assertThat(rejected("invalid_fields") - invalid).isGreaterThanOrEqualTo(1);
        assertThat(rejected("unparseable") - unparseable).isGreaterThanOrEqualTo(1);
        assertThat(meters.get("trackstore.updates.age").timer().count() - ages).isGreaterThanOrEqualTo(2);
    }

    @Test
    void invalidValuesAreDeadLetteredWithOriginalPayloadAndNeverStored() {
        String payload = update(t0, 91.0, "FRIEND");   // latitude out of range

        publish(payload);

        ConsumerRecord<String, String> deadLetter = awaitDeadLetter();
        assertThat(deadLetter.value()).isEqualTo(payload);
        assertThat(header(deadLetter, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).endsWith("MethodArgumentNotValidException");
        assertNothingStored();
    }

    @Test
    void unknownIdentityIsDeadLetteredAsUnparseable() {
        String payload = update(t0, 37.5, "ENEMY");

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

    @Test
    void quietTrackIsMarkedStaleByTheScheduledJob() {
        publish(update(t0.minusSeconds(12), 37.5, "FRIEND"));   // older than stale-after (10 s)

        // Applied as ACTIVE; the expiry job (every 2 s in the running app) then marks it STALE.
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(picture.findById(trackId).orElseThrow().getStatus()).isEqualTo(TrackStatus.STALE));
    }

    @Test
    void updateAlreadyPastDropAfterIsKeptInHistoryOnly() {
        publish(update(t0.minusSeconds(60), 37.5, "FRIEND"));   // older than drop-after (30 s)
        publish("not json");                                    // barrier, see above
        awaitDeadLetter();

        assertThat(picture.findById(trackId)).isEmpty();
        assertThat(historyOfTrack()).hasSize(1);
    }

    // --- helpers -------------------------------------------------------------------------------

    private double updates(String outcome) {
        return meters.get("trackstore.updates").tag("outcome", outcome).counter().count();
    }

    private double rejected(String reason) {
        return meters.get("trackstore.updates.rejected").tag("reason", reason).counter().count();
    }

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
        return history.findHistory(trackId, null, null);
    }

    /** Safe right after {@link #awaitDeadLetter()}: the rejected record has been fully handled by then. */
    private void assertNothingStored() {
        assertThat(picture.findById(trackId)).isEmpty();
        assertThat(historyOfTrack()).isEmpty();
    }
}
