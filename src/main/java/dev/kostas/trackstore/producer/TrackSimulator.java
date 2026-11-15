package dev.kostas.trackstore.producer;

import dev.kostas.trackstore.track.Identity;
import dev.kostas.trackstore.track.TrackUpdate;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

/**
 * A handful of tracks flying in straight lines at constant speed, for the demo producer.
 * <p>
 * Stateless and deterministic: the position at any moment is computed from the start position,
 * heading, speed and elapsed time, so the same inputs always give the same updates (easy to test).
 * Uses a flat-earth approximation, which is plenty for a demo that covers a few kilometres.
 */
final class TrackSimulator {

    private static final double METRES_PER_DEGREE_LATITUDE = 111_320.0;
    private static final double START_LATITUDE = 37.90;   // around Athens
    private static final double START_LONGITUDE = 23.70;

    /** The kinds of object we simulate; cycled when more tracks are requested. */
    private static final List<Kind> KINDS = List.of(
            new Kind(Identity.FRIEND, 10_000.0, 230.0),    // airliner
            new Kind(Identity.UNKNOWN, 1_500.0, 40.0),     // UAV
            new Kind(Identity.HOSTILE, 8_000.0, 300.0),    // fighter
            new Kind(Identity.NEUTRAL, null, 60.0),        // helicopter, altitude not reported
            new Kind(Identity.HOSTILE, 3_000.0, 680.0));   // missile

    private record Kind(Identity identity, Double altitude, double speedMetresPerSecond) {
    }

    record SimulatedTrack(String trackId, double latitude, double longitude, double heading,
                          double speedMetresPerSecond, Double altitude, Identity identity) {

        /** Where this track is after flying for {@code elapsed}, observed at {@code timestamp}. */
        TrackUpdate observedAt(Instant timestamp, Duration elapsed) {
            double distance = speedMetresPerSecond * elapsed.toNanos() / 1e9;
            double headingRadians = Math.toRadians(heading);
            double lat = latitude + distance * Math.cos(headingRadians) / METRES_PER_DEGREE_LATITUDE;
            double lon = longitude + distance * Math.sin(headingRadians)
                    / (METRES_PER_DEGREE_LATITUDE * Math.cos(Math.toRadians(latitude)));
            return new TrackUpdate(trackId, timestamp, round(lat), round(lon), altitude, heading, identity);
        }
    }

    private final List<SimulatedTrack> tracks;

    TrackSimulator(int trackCount) {
        this.tracks = IntStream.range(0, trackCount).mapToObj(TrackSimulator::track).toList();
    }

    List<SimulatedTrack> tracks() {
        return tracks;
    }

    /** One update per track: where each track is {@code elapsed} after {@code start}. */
    List<TrackUpdate> observe(Instant start, Duration elapsed) {
        Instant timestamp = start.plus(elapsed);
        return tracks.stream().map(track -> track.observedAt(timestamp, elapsed)).toList();
    }

    private static SimulatedTrack track(int index) {
        Kind kind = KINDS.get(index % KINDS.size());
        return new SimulatedTrack(
                "TRK-%03d".formatted(index + 1),
                START_LATITUDE + 0.1 * index,
                START_LONGITUDE + 0.1 * index,
                (45.0 + 72.0 * index) % 360.0,   // spread the headings
                kind.speedMetresPerSecond(),
                kind.altitude(),
                kind.identity());
    }

    /** Six decimals ≈ 10 cm: precise enough, and keeps the JSON readable. */
    private static double round(double degrees) {
        return Math.round(degrees * 1e6) / 1e6;
    }
}
