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
 * Turns the exception behind a rejected record into a short, human-readable reason for the log,
 * e.g. {@code "invalid fields: latitude must be less than or equal to 90.0 (was 91.0)"}.
 */
final class RejectionReason {

    private RejectionReason() {
    }

    static String describe(Throwable exception) {
        Throwable cause = unwrap(exception);

        if (cause instanceof MethodArgumentNotValidException invalid && invalid.getBindingResult() != null) {
            return "invalid fields: " + describe(invalid.getBindingResult());
        }
        if (cause instanceof ConversionException || cause instanceof MessageConversionException) {
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
