package dev.kostas.trackstore.web;

import dev.kostas.trackstore.track.TrackNotFoundException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Duration;

/**
 * The one place where exceptions from the services become HTTP responses (RFC 9457 bodies).
 * <p>
 * The services and the domain know nothing about HTTP: they throw domain exceptions
 * ({@link TrackNotFoundException}) or let Spring's data-access exceptions through, and this advice
 * picks the status. Spring MVC's own errors (bad parameter, unknown URL, ...) are still handled by
 * Spring Boot ({@code spring.mvc.problemdetails.enabled}).
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    static final Duration RETRY_AFTER = Duration.ofSeconds(5);

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    /** Unknown track → {@code 404}. An expected answer, so no log line. */
    @ExceptionHandler(TrackNotFoundException.class)
    public ProblemDetail trackNotFound(TrackNotFoundException ex) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    /**
     * Database unreachable → {@code 503} with {@code Retry-After}: the request was fine and the
     * outage is temporary. How fast this answer comes is set by
     * {@code spring.datasource.hikari.connection-timeout}.
     * <p>
     * {@link CannotCreateTransactionException}: no connection to start a transaction (the read
     * services are transactional). {@link DataAccessResourceFailureException}: no connection, or the
     * connection was lost, outside such a transaction (includes {@code CannotGetJdbcConnectionException}).
     * <p>
     * The body names the dependency but not the cause: exception messages can contain host names and
     * pool details, so they go to the log only ({@code /health} shows operators the database status).
     */
    @ExceptionHandler({CannotCreateTransactionException.class, DataAccessResourceFailureException.class})
    public ResponseEntity<ProblemDetail> databaseUnavailable(Exception ex, HttpServletRequest request) {
        // One line per request, not a stack trace: during an outage every request ends up here.
        log.warn("Database unavailable for {} {}: {}", request.getMethod(), request.getRequestURI(),
                NestedExceptionUtils.getMostSpecificCause(ex).toString());

        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "The track database is unavailable. Try again shortly.");
        problem.setProperty("dependency", "database");

        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .header(HttpHeaders.RETRY_AFTER, String.valueOf(RETRY_AFTER.toSeconds()))
                .body(problem);
    }
}
