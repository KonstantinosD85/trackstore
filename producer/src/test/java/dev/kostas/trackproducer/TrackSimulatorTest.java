package dev.kostas.trackproducer;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.within;

class TrackSimulatorTest {

    private static final Instant T0 = Instant.parse("2026-01-01T12:00:00Z");

    @Test
    void trackFlyingNorthGainsLatitudeOnly() {
        var track = new TrackSimulator.SimulatedTrack("X", 10.0, 20.0, 0.0, 100.0, null, Identity.FRIEND);

        TrackUpdateMessage update = track.observedAt(T0, Duration.ofSeconds(1_113));   // ≈ 111.3 km

        assertThat(update.latitude()).isCloseTo(11.0, within(0.001));
        assertThat(update.longitude()).isEqualTo(20.0);
    }

    @Test
    void trackFlyingEastGainsLongitudeOnly() {
        var track = new TrackSimulator.SimulatedTrack("X", 0.0, 20.0, 90.0, 100.0, null, Identity.FRIEND);

        TrackUpdateMessage update = track.observedAt(T0, Duration.ofSeconds(1_113));

        assertThat(update.latitude()).isCloseTo(0.0, within(1e-6));
        assertThat(update.longitude()).isCloseTo(21.0, within(0.001));
    }

    @Test
    void observeGivesOneUpdatePerTrackAtStartPlusElapsed() {
        List<TrackUpdateMessage> updates = new TrackSimulator(3).observe(T0, Duration.ofSeconds(5));

        assertThat(updates).extracting(TrackUpdateMessage::trackId).containsExactly("TRK-001", "TRK-002", "TRK-003");
        assertThat(updates).extracting(TrackUpdateMessage::timestamp).containsOnly(T0.plusSeconds(5));
    }

    @Test
    void isDeterministic() {
        assertThat(new TrackSimulator(5).observe(T0, Duration.ofMinutes(1)))
                .isEqualTo(new TrackSimulator(5).observe(T0, Duration.ofMinutes(1)));
    }

    @Test
    void producesOnlyValidUpdatesForAnHourOfTheMaximumNumberOfTracks() {
        TrackSimulator simulator = new TrackSimulator(TrackSimulator.MAX_TRACKS);

        for (int minute = 0; minute <= 60; minute++) {
            assertThat(simulator.observe(T0, Duration.ofMinutes(minute)))
                    .as("minute %d", minute)
                    .allMatch(MessageContract::isValid);
        }
    }

    @Test
    void rejectsTrackCountOutsideRange() {
        assertThatIllegalArgumentException().isThrownBy(() -> new TrackSimulator(0));
        assertThatIllegalArgumentException().isThrownBy(() -> new TrackSimulator(TrackSimulator.MAX_TRACKS + 1));
    }
}
