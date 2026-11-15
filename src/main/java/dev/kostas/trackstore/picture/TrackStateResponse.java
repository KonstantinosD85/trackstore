package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.track.Identity;

import java.time.Instant;

/**
 * The latest state of one track, as returned by the read API.
 * <p>
 * A separate type from the {@link TrackState} entity, so the API contract does not change when the
 * table does. Field names follow the incoming track update ({@code timestamp}, not {@code observedAt}).
 *
 * @param timestamp      when the sensor observed the track (from the update)
 * @param lastReceivedAt when this service applied that update
 * @param status         {@code ACTIVE}, or {@code STALE} when no observation arrived for a while
 */
public record TrackStateResponse(
        String trackId,
        Instant timestamp,
        double latitude,
        double longitude,
        Double altitude,
        Double heading,
        Identity identity,
        Instant lastReceivedAt,
        TrackStatus status) {

    static TrackStateResponse from(TrackState state) {
        return new TrackStateResponse(
                state.getTrackId(),
                state.getObservedAt(),
                state.getLatitude(),
                state.getLongitude(),
                state.getAltitude(),
                state.getHeading(),
                state.getIdentity(),
                state.getLastReceivedAt(),
                state.getStatus());
    }
}
