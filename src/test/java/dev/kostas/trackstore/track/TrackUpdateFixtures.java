package dev.kostas.trackstore.track;

import java.time.Instant;

/**
 * Test data builder for {@link TrackUpdate}.
 * <p>
 * Starts from a fully valid update; tests override only the field they care about,
 * e.g. {@code aTrackUpdate().latitude(91.0).build()}.
 */
public final class TrackUpdateFixtures {

    private TrackUpdateFixtures() {
    }

    public static Builder aTrackUpdate() {
        return new Builder();
    }

    public static final class Builder {

        private String trackId = "TRK-001";
        private Instant timestamp = Instant.parse("2026-01-01T12:00:00Z");
        private Double latitude = 37.9838;
        private Double longitude = 23.7275;
        private Double altitude = 10_000.0;
        private Double heading = 90.0;
        private Identity identity = Identity.UNKNOWN;

        public Builder trackId(String trackId) {
            this.trackId = trackId;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder latitude(Double latitude) {
            this.latitude = latitude;
            return this;
        }

        public Builder longitude(Double longitude) {
            this.longitude = longitude;
            return this;
        }

        public Builder altitude(Double altitude) {
            this.altitude = altitude;
            return this;
        }

        public Builder heading(Double heading) {
            this.heading = heading;
            return this;
        }

        public Builder identity(Identity identity) {
            this.identity = identity;
            return this;
        }

        public TrackUpdate build() {
            return new TrackUpdate(trackId, timestamp, latitude, longitude, altitude, heading, identity);
        }
    }
}
