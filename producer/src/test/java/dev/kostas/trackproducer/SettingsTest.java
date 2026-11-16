package dev.kostas.trackproducer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class SettingsTest {

    private static final String[] NO_ARGS = {};

    @Test
    void defaults() {
        assertThat(Settings.parse(NO_ARGS, Map.of()))
                .isEqualTo(new Settings("localhost:9092", "track-updates", 3, 30, Duration.ofSeconds(1)));
    }

    @Test
    void commandLineOverridesEnvironmentWhichOverridesDefaults() {
        Settings settings = Settings.parse(
                new String[]{"--ticks=60"},
                Map.of("PRODUCER_TICKS", "10", "PRODUCER_BOOTSTRAP_SERVERS", "kafka:29092"));

        assertThat(settings.ticks()).isEqualTo(60);
        assertThat(settings.bootstrapServers()).isEqualTo("kafka:29092");
        assertThat(settings.tracks()).isEqualTo(3);
    }

    @Test
    void zeroTicksMeansRunUntilStopped() {
        assertThat(Settings.parse(new String[]{"--ticks=0"}, Map.of()).runsUntilStopped()).isTrue();
    }

    @Test
    void parsesSimpleAndIsoDurations() {
        assertThat(Settings.duration("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(Settings.duration("2s")).isEqualTo(Duration.ofSeconds(2));
        assertThat(Settings.duration("1m")).isEqualTo(Duration.ofMinutes(1));
        assertThat(Settings.duration("PT1.5S")).isEqualTo(Duration.ofMillis(1500));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "--ticks=5",          // fewer than needed for all anomalies
            "--ticks=-1",
            "--ticks=many",
            "--tracks=0",
            "--tracks=11",
            "--interval=0s",
            "--interval=soon",
            "--speed=fast",       // unknown option
            "ticks=10"            // missing --
    })
    void rejectsInvalidOptions(String arg) {
        assertThatIllegalArgumentException().isThrownBy(() -> Settings.parse(new String[]{arg}, Map.of()));
    }

    @Test
    void usageListsEveryOptionWithItsEnvironmentVariable() {
        assertThat(Settings.usage()).contains("--ticks", "PRODUCER_TICKS", "--bootstrap-servers", "PRODUCER_BOOTSTRAP_SERVERS");
    }
}
