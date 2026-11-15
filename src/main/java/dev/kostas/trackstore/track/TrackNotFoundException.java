package dev.kostas.trackstore.track;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.ErrorResponseException;

/**
 * Thrown by the read API when a {@code trackId} has never been seen.
 * <p>
 * As an {@link ErrorResponseException} it carries its own HTTP status and RFC 9457 body, so Spring
 * MVC renders it as {@code 404} with {@code application/problem+json} — no handler code needed.
 */
public class TrackNotFoundException extends ErrorResponseException {

    public TrackNotFoundException(String trackId) {
        super(HttpStatus.NOT_FOUND,
                ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, "No track with id '" + trackId + "'"),
                null);
    }
}
