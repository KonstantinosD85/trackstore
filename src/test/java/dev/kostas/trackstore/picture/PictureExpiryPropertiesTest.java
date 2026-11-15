package dev.kostas.trackstore.picture;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Binding and validation of {@code trackstore.picture.*}.
 * <p>
 * {@link ApplicationContextRunner} starts a tiny Spring context with only the given configuration
 * and properties — milliseconds, no database — and lets us assert that it started or failed.
 */
class PictureExpiryPropertiesTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(PictureConfig.class);

    @Test
    void bindsDurations() {
        context.withPropertyValues(
                        "trackstore.picture.stale-after=10s",
                        "trackstore.picture.drop-after=30s",
                        "trackstore.picture.check-interval=2s")
                .run(ctx -> assertThat(ctx.getBean(PictureExpiryProperties.class))
                        .isEqualTo(new PictureExpiryProperties(
                                Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(2))));
    }

    @Test
    void failsToStartWhenDropAfterIsNotLongerThanStaleAfter() {
        context.withPropertyValues(
                        "trackstore.picture.stale-after=30s",
                        "trackstore.picture.drop-after=10s",
                        "trackstore.picture.check-interval=2s")
                .run(ctx -> assertThat(ctx).hasFailed()
                        .getFailure().rootCause().hasMessageContaining("drop-after must be longer than stale-after"));
    }

    @Test
    void failsToStartWhenCheckIntervalIsNotShorterThanStaleAfter() {
        context.withPropertyValues(
                        "trackstore.picture.stale-after=10s",
                        "trackstore.picture.drop-after=30s",
                        "trackstore.picture.check-interval=10s")
                .run(ctx -> assertThat(ctx).hasFailed()
                        .getFailure().rootCause().hasMessageContaining("check-interval must be shorter than stale-after"));
    }

    @Test
    void failsToStartWhenASettingIsMissing() {
        context.withPropertyValues("trackstore.picture.stale-after=10s")
                .run(ctx -> assertThat(ctx).hasFailed());
    }
}
