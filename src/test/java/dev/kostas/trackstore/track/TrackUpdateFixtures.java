package dev.kostas.trackstore.track;

import java.time.Instant;

/**
 * Test data builder for {@link TrackUpdate}.
 * <p>
 * Starts from a fully valid update; tests override only the field they care about,
 * e.g. {@code aTrackUpdate().latitude(91.0).build()}.
 */
final class TrackUpdateFixtures {

    private TrackUpdateFixtures() {
    }

    static Builder aTrackUpdate() {
        return new Builder();
    }

    static final class Builder {

        private String trackId = "TRK-001";
        private Instant timestamp = Instant.parse("2026-01-01T12:00:00Z");
        private Double latitude = 37.9838;
        private Double longitude = 23.7275;
        private Double altitude = 10_000.0;
        private Double heading = 90.0;
        private Identity identity = Identity.UNKNOWN;

        Builder trackId(String trackId) {
            this.trackId = trackId;
            return this;
        }

        Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        Builder latitude(Double latitude) {
            this.latitude = latitude;
            return this;
        }

        Builder longitude(Double longitude) {
            this.longitude = longitude;
            return this;
        }

        Builder altitude(Double altitude) {
            this.altitude = altitude;
            return this;
        }

        Builder heading(Double heading) {
            this.heading = heading;
            return this;
        }

        Builder identity(Identity identity) {
            this.identity = identity;
            return this;
        }

        TrackUpdate build() {
            return new TrackUpdate(trackId, timestamp, latitude, longitude, altitude, heading, identity);
        }
    }
}
