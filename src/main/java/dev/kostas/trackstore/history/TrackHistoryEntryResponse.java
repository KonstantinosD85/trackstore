package dev.kostas.trackstore.history;

import dev.kostas.trackstore.track.Identity;

import java.time.Instant;

/**
 * One past observation of a track, as returned by the read API. The {@code trackId} is left out:
 * the caller already named it in the URL.
 *
 * @param timestamp  when the sensor observed the track (from the update); history is ordered by this
 * @param receivedAt when this service stored it — differs from the order of {@code timestamp}
 *                   for updates that arrived out of order
 */
public record TrackHistoryEntryResponse(
        Instant timestamp,
        double latitude,
        double longitude,
        Double altitude,
        Double heading,
        Identity identity,
        Instant receivedAt) {

    static TrackHistoryEntryResponse from(TrackHistoryEntry entry) {
        return new TrackHistoryEntryResponse(
                entry.getObservedAt(),
                entry.getLatitude(),
                entry.getLongitude(),
                entry.getAltitude(),
                entry.getHeading(),
                entry.getIdentity(),
                entry.getReceivedAt());
    }
}
