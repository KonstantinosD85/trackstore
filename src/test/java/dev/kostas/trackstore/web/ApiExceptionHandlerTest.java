package dev.kostas.trackstore.web;

import dev.kostas.trackstore.history.TrackHistoryController;
import dev.kostas.trackstore.history.TrackHistoryQueryService;
import dev.kostas.trackstore.picture.PictureController;
import dev.kostas.trackstore.picture.PictureQueryService;
import dev.kostas.trackstore.track.TrackNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.assertj.MockMvcTester;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLTransientConnectionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;

/**
 * Exceptions from the services → HTTP responses.
 * <p>
 * A web slice ({@link WebMvcTest}): only the controllers and the advice, with the services mocked
 * to throw. The 404 is also covered end to end by the controller tests; the database outage was
 * checked live (stopping the shared Testcontainers database would break every other test).
 */
@WebMvcTest({PictureController.class, TrackHistoryController.class})
class ApiExceptionHandlerTest {

    /** What the real exceptions carry; it must not reach the client. */
    private static final String INTERNAL_CAUSE =
            "HikariPool-1 - Connection is not available, request timed out after 2001ms (localhost:5432)";

    @Autowired
    private MockMvcTester mvc;

    @MockitoBean
    private PictureQueryService picture;

    @MockitoBean
    private TrackHistoryQueryService history;

    @Test
    void unknownTrackIs404() {
        given(picture.track("NOPE")).willThrow(new TrackNotFoundException("NOPE"));

        assertThat(mvc.get().uri("/tracks/{id}", "NOPE"))
                .hasStatus(HttpStatus.NOT_FOUND)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .bodyJson().isLenientlyEqualTo("""
                        { "status": 404, "title": "Not Found", "detail": "No track with id 'NOPE'" }
                        """);
    }

    @Test
    void noTransactionBecauseNoConnectionIs503() {
        given(picture.allTracks()).willThrow(new CannotCreateTransactionException(
                "Could not open JPA EntityManager for transaction", new SQLTransientConnectionException(INTERNAL_CAUSE)));

        assertThat(mvc.get().uri("/picture"))
                .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .hasContentType(MediaType.APPLICATION_PROBLEM_JSON)
                .hasHeader(HttpHeaders.RETRY_AFTER, "5")
                .bodyJson().isLenientlyEqualTo("""
                        {
                          "title": "Service Unavailable",
                          "status": 503,
                          "detail": "The track database is unavailable. Try again shortly.",
                          "dependency": "database"
                        }
                        """);
    }

    @Test
    void noConnectionOutsideATransactionIs503() {
        given(history.history(anyString(), any(), any())).willThrow(new CannotGetJdbcConnectionException(
                "Failed to obtain JDBC Connection", new SQLTransientConnectionException(INTERNAL_CAUSE)));

        assertThat(mvc.get().uri("/tracks/{id}/history", "TRK-1"))
                .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .bodyJson().extractingPath("$.dependency").isEqualTo("database");
    }

    @Test
    void internalDetailsStayInTheLog() {
        given(picture.track(anyString())).willThrow(new CannotCreateTransactionException(
                "Could not open JPA EntityManager for transaction", new SQLTransientConnectionException(INTERNAL_CAUSE)));

        assertThat(mvc.get().uri("/tracks/{id}", "TRK-1"))
                .hasStatus(HttpStatus.SERVICE_UNAVAILABLE)
                .bodyText().doesNotContain("Hikari", "localhost", "JPA", "Exception");
    }
}
