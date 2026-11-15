package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.KafkaTestcontainersConfiguration;
import dev.kostas.trackstore.TestcontainersConfiguration;
import dev.kostas.trackstore.track.Identity;
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
 * The picture endpoints against the real application and PostgreSQL.
 * <p>
 * MockMvc calls the controllers in the test's own thread, so {@link Transactional} covers both the
 * test data and the request — everything is rolled back afterwards. Track ids are still unique,
 * because other test classes (the Kafka listener tests) commit tracks to the same database.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import({TestcontainersConfiguration.class, KafkaTestcontainersConfiguration.class})
@Transactional
class PictureControllerTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Autowired
    private MockMvcTester mvc;

    @Autowired
    private TrackStateRepository picture;

    private final String prefix = "TRK-" + UUID.randomUUID() + "-";

    @Test
    void trackReturnsLatestStateAsJson() {
        String trackId = prefix + "A";
        picture.upsertIfNewer(aTrackUpdate().trackId(trackId).timestamp(T0).latitude(10.0).build());
        picture.upsertIfNewer(aTrackUpdate().trackId(trackId).timestamp(T0.plusSeconds(5))
                .latitude(37.5).longitude(23.5).altitude(9_000.0).heading(270.0)
                .identity(Identity.HOSTILE).build());

        assertThat(mvc.get().uri("/tracks/{trackId}", trackId))
                .hasStatusOk()
                .hasContentType(MediaType.APPLICATION_JSON)
                .bodyJson().isLenientlyEqualTo("""
                        {
                          "trackId": "%s",
                          "timestamp": "2026-01-01T12:00:05Z",
                          "latitude": 37.5,
                          "longitude": 23.5,
                          "altitude": 9000.0,
                          "heading": 270.0,
                          "identity": "HOSTILE"
                        }
                        """.formatted(trackId));
    }

    @Test
    void missingOptionalFieldsAreReturnedAsNull() {
        String trackId = prefix + "A";
        picture.upsertIfNewer(aTrackUpdate().trackId(trackId).altitude(null).heading(null).build());

        assertThat(mvc.get().uri("/tracks/{trackId}", trackId))
                .hasStatusOk()
                .bodyJson().isLenientlyEqualTo("""
                        { "altitude": null, "heading": null }
                        """);
    }

    @Test
    void unknownTrackIs404ProblemDetail() {
        assertThat(mvc.get().uri("/tracks/{trackId}", "NOPE"))
                .hasStatus(HttpStatus.NOT_FOUND)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().isLenientlyEqualTo("""
                        { "status": 404, "title": "Not Found", "detail": "No track with id 'NOPE'" }
                        """);
    }

    @Test
    void pictureReturnsEveryTrackSortedById() {
        picture.upsertIfNewer(aTrackUpdate().trackId(prefix + "B").build());
        picture.upsertIfNewer(aTrackUpdate().trackId(prefix + "A").build());

        assertThat(mvc.get().uri("/picture"))
                .hasStatusOk()
                .bodyJson()
                // Other test classes may have committed tracks too; look only at ours.
                .extractingPath("$[?(@.trackId =~ /%s.*/)].trackId".formatted(prefix))
                .asArray()
                .containsExactly(prefix + "A", prefix + "B");
    }
}
