package dev.kostas.trackstore.history;

import dev.kostas.trackstore.KafkaTestcontainersConfiguration;
import dev.kostas.trackstore.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

import static dev.kostas.trackstore.track.TrackUpdateFixtures.aTrackUpdate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The history endpoint against the real application and PostgreSQL; rolled back after each test
 * (see {@code PictureControllerTest} for why that works with MockMvc).
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import({TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class})
@Transactional
class TrackHistoryControllerTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Autowired
    private MockMvcTester mvc;

    @Autowired
    private TrackHistoryRepository history;

    private final String trackId = "TRK-" + UUID.randomUUID();

    @BeforeEach
    void storeHistoryArrivingOutOfOrder() {
        append(T0.plusSeconds(20));
        append(T0);
        append(T0.plusSeconds(10));
    }

    @Test
    void withoutFiltersReturnsWholeHistoryInObservationOrder() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId))
                .hasStatusOk()
                .hasContentType(MediaType.APPLICATION_JSON)
                .bodyJson().extractingPath("$[*].timestamp").asArray()
                .containsExactly("2026-01-01T12:00:00Z", "2026-01-01T12:00:10Z", "2026-01-01T12:00:20Z");
    }

    @Test
    void fromIsInclusiveAndToIsExclusive() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId)
                .param("from", "2026-01-01T12:00:10Z")
                .param("to", "2026-01-01T12:00:20Z"))
                .hasStatusOk()
                .bodyJson().extractingPath("$[*].timestamp").asArray()
                .containsExactly("2026-01-01T12:00:10Z");
    }

    @Test
    void eachBoundCanBeGivenAlone() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId).param("from", "2026-01-01T12:00:10Z"))
                .bodyJson().extractingPath("$[*].timestamp").asArray()
                .containsExactly("2026-01-01T12:00:10Z", "2026-01-01T12:00:20Z");

        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId).param("to", "2026-01-01T12:00:10Z"))
                .bodyJson().extractingPath("$[*].timestamp").asArray()
                .containsExactly("2026-01-01T12:00:00Z");
    }

    @Test
    void knownTrackWithNothingInRangeIsEmptyList() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId).param("from", "2027-01-01T00:00:00Z"))
                .hasStatusOk()
                .bodyJson().isEqualTo("[]");
    }

    @Test
    void unknownTrackIs404() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", "NOPE"))
                .hasStatus(HttpStatus.NOT_FOUND)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
    }

    @Test
    void fromAfterToIs400() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId)
                .param("from", "2026-01-01T12:00:20Z")
                .param("to", "2026-01-01T12:00:00Z"))
                .hasStatus(HttpStatus.BAD_REQUEST)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().extractingPath("$.detail").isEqualTo("'from' must not be after 'to'");
    }

    @Test
    void unparseableTimestampIs400() {
        assertThat(mvc.get().uri("/tracks/{trackId}/history", trackId).param("from", "yesterday"))
                .hasStatus(HttpStatus.BAD_REQUEST)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON);
    }

    private void append(Instant timestamp) {
        history.appendIfAbsent(aTrackUpdate().trackId(trackId).timestamp(timestamp).build());
    }
}
