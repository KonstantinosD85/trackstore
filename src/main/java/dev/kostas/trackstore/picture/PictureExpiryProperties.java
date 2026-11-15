package dev.kostas.trackstore.picture;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.time.Instant;

/**
 * When tracks go stale and when they are dropped from the picture, bound from
 * {@code trackstore.picture.*}. Ages are measured from the track's latest <em>observation</em>
 * timestamp, so the status says how old the shown position is — also after a replay or a backlog.
 *
 * @param staleAfter    no observation for this long → {@link TrackStatus#STALE}
 * @param dropAfter     no observation for this long → removed from the picture
 * @param checkInterval how often {@link PictureExpiryJob} runs
 */
@ConfigurationProperties("trackstore.picture")
@Validated
public record PictureExpiryProperties(
        @NotNull Duration staleAfter,
        @NotNull Duration dropAfter,
        @NotNull Duration checkInterval) {

    /** Observations older than this no longer belong in the picture. */
    public Instant dropCutoff(Instant now) {
        return now.minus(dropAfter);
    }

    public Instant staleCutoff(Instant now) {
        return now.minus(staleAfter);
    }

    @AssertTrue(message = "drop-after must be longer than stale-after")
    boolean isDropAfterLongerThanStaleAfter() {
        return staleAfter == null || dropAfter == null || dropAfter.compareTo(staleAfter) > 0;
    }

    @AssertTrue(message = "check-interval must be shorter than stale-after")
    boolean isCheckIntervalShorterThanStaleAfter() {
        return staleAfter == null || checkInterval == null || checkInterval.compareTo(staleAfter) < 0;
    }
}
