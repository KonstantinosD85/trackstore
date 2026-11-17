package dev.kostas.trackstore.ingest;

import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;

import java.util.Comparator;
import java.util.stream.Collectors;

/**
 * Explains the exception behind a rejected record, two ways:
 * <ul>
 *   <li>{@link #describe}: a detailed, human-readable reason for the log, e.g.
 *       {@code "invalid fields: latitude must be less than or equal to 90.0 (was 91.0)"};</li>
 *   <li>{@link #categorize}: one of a few fixed {@link Category categories}, for a metric tag
 *       (a tag must have few distinct values, so never the detailed text).</li>
 * </ul>
 */
final class RejectionReason {

    /** Why a record was rejected, coarse enough to be a metric tag. */
    enum Category {
        /** Valid JSON, but a field breaks a validation rule (e.g. latitude 91). */
        INVALID_FIELDS,
        /** Not readable as a track update (malformed JSON, unknown identity, bad timestamp). */
        UNPARSEABLE,
        /** Anything else the error handler gave up on. */
        OTHER;

        /** Tag value, e.g. {@code "invalid_fields"}. */
        String tag() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    private RejectionReason() {
    }

    static Category categorize(Throwable exception) {
        Throwable cause = unwrap(exception);
        if (cause instanceof MethodArgumentNotValidException) {
            return Category.INVALID_FIELDS;
        }
        if (cause instanceof ConversionException || cause instanceof MessageConversionException) {
            return Category.UNPARSEABLE;
        }
        return Category.OTHER;
    }

    static String describe(Throwable exception) {
        Throwable cause = unwrap(exception);

        if (cause instanceof MethodArgumentNotValidException invalid && invalid.getBindingResult() != null) {
            return "invalid fields: " + describe(invalid.getBindingResult());
        }
        if (categorize(cause) == Category.UNPARSEABLE) {
            // The root cause is Jackson's, e.g. "Cannot deserialize value of type `Identity` from String "ENEMY" ..."
            return "unparseable payload: " + firstLine(NestedExceptionUtils.getMostSpecificCause(cause).getMessage());
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }

    /** Jackson appends source-location details on further lines; the first line is the reason. */
    private static String firstLine(String message) {
        return message == null ? "" : message.lines().findFirst().orElse("");
    }

    /** Spring Kafka wraps listener failures; the interesting exception is inside. */
    private static Throwable unwrap(Throwable exception) {
        Throwable cause = exception;
        while (cause instanceof ListenerExecutionFailedException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private static String describe(BindingResult result) {
        return result.getFieldErrors().stream()
                .sorted(Comparator.comparing(FieldError::getField)) // stable order → predictable log lines
                .map(error -> error.getField() + " " + error.getDefaultMessage()
                        + " (was " + error.getRejectedValue() + ")")
                .collect(Collectors.joining(", "));
    }
}
