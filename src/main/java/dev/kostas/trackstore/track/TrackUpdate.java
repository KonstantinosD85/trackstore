package dev.kostas.trackstore.track;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

import java.time.Instant;

/**
 * One observation of one track, as received on the {@code track-updates} topic.
 * <p>
 * Numeric fields use wrapper types ({@link Double}) so that a missing JSON field becomes
 * {@code null} and is caught by {@link NotNull}, instead of silently defaulting to {@code 0.0}.
 * Optional fields ({@code altitude}, {@code heading}) may be {@code null}; their range
 * constraints only apply when a value is present.
 */
public record TrackUpdate(

        @NotBlank
        String trackId,

        @NotNull
        Instant timestamp,

        @NotNull
        @DecimalMin("-90.0")
        @DecimalMax("90.0")
        Double latitude,

        @NotNull
        @DecimalMin("-180.0")
        @DecimalMax("180.0")
        Double longitude,

        @PositiveOrZero
        Double altitude,

        @DecimalMin("0.0")
        @DecimalMax(value = "360.0", inclusive = false)
        Double heading,

        @NotNull
        Identity identity
) {
}
