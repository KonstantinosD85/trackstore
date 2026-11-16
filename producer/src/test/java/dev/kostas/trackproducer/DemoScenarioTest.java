package dev.kostas.trackproducer;

import dev.kostas.trackproducer.DemoScenario.Kind;
import dev.kostas.trackproducer.DemoScenario.Message;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DemoScenarioTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");
    private static final int TRACKS = 3;
    private static final int TICKS = 10;

    private final JsonMapper json = JsonMapper.builder().build();

    /** All messages of {@value #TICKS} ticks, in publishing order. */
    private final List<Message> messages = messagesOf(new DemoScenario(new TrackSimulator(TRACKS), json, T0, Duration.ofSeconds(1)));

    private static List<Message> messagesOf(DemoScenario scenario) {
        return IntStream.range(0, TICKS).mapToObj(tick -> scenario.nextTick()).flatMap(List::stream).toList();
    }

    @Test
    void regularUpdatesAreValidAndKeyedByTrackId() {
        assertThat(ofKind(Kind.UPDATE)).hasSize(TRACKS * TICKS - 1)   // one is the out-of-order one
                .allSatisfy(message -> {
                    TrackUpdateMessage update = parse(message);
                    assertThat(MessageContract.isValid(update)).as(message.payload()).isTrue();
                    assertThat(message.key()).isEqualTo(update.trackId());
                });
    }

    @Test
    void timestampsAreIsoStringsAsTheContractRequires() {
        assertThat(messages.getFirst().payload()).contains("\"timestamp\":\"2026-01-01T12:00:00Z\"");
    }

    @Test
    void containsOneExactDuplicateRightAfterTheOriginal() {
        assertThat(ofKind(Kind.DUPLICATE)).singleElement().satisfies(duplicate -> {
            Message previous = messages.get(messages.indexOf(duplicate) - 1);
            assertThat(duplicate.key()).isEqualTo(previous.key());
            assertThat(duplicate.payload()).isEqualTo(previous.payload());
        });
    }

    @Test
    void containsOneUpdateOlderThanOneAlreadySentForTheSameTrack() {
        assertThat(ofKind(Kind.OUT_OF_ORDER)).singleElement().satisfies(late -> {
            Instant newestBefore = messages.subList(0, messages.indexOf(late)).stream()
                    .filter(m -> m.key().equals(late.key()) && m.kind() != Kind.INVALID)
                    .map(m -> parse(m).timestamp())
                    .max(Instant::compareTo).orElseThrow();
            assertThat(parse(late).timestamp()).isBefore(newestBefore);
            assertThat(MessageContract.isValid(parse(late))).isTrue();
        });
    }

    @Test
    void containsOneInvalidAndOneUnparseableMessage() {
        List<Message> invalid = ofKind(Kind.INVALID);

        assertThat(invalid).hasSize(2);
        assertThat(MessageContract.isValid(parse(invalid.get(0)))).isFalse();
        assertThatThrownBy(() -> parse(invalid.get(1))).isInstanceOf(JacksonException.class);
    }

    @Test
    void everyTrackHasOneValidObservationPerTick() {
        Map<String, Long> distinctTimestamps = messages.stream()
                .filter(m -> m.kind() != Kind.INVALID)
                .collect(Collectors.groupingBy(Message::key,
                        Collectors.mapping(m -> parse(m).timestamp(),
                                Collectors.collectingAndThen(Collectors.toSet(), s -> (long) s.size()))));

        assertThat(distinctTimestamps).hasSize(TRACKS).allSatisfy((track, count) -> assertThat(count).isEqualTo(TICKS));
    }

    @Test
    void anomaliesHappenOnceThenTracksJustKeepMoving() {
        DemoScenario scenario = new DemoScenario(new TrackSimulator(TRACKS), json, T0, Duration.ofSeconds(1));
        IntStream.range(0, DemoScenario.MIN_TICKS).forEach(tick -> scenario.nextTick());

        for (int tick = 0; tick < 20; tick++) {
            assertThat(scenario.nextTick()).hasSize(TRACKS).extracting(Message::kind).containsOnly(Kind.UPDATE);
        }
    }

    private List<Message> ofKind(Kind kind) {
        return messages.stream().filter(m -> m.kind() == kind).toList();
    }

    private TrackUpdateMessage parse(Message message) {
        return json.readValue(message.payload(), TrackUpdateMessage.class);
    }
}
