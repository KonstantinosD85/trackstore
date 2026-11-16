package dev.kostas.trackproducer;

import java.time.Instant;

/**
 * One message on the {@code track-updates} topic, as JSON. This is the producer's own copy of the
 * message contract: it deliberately does not share code with the service, exactly like an external
 * sensor feed wouldn't. The JSON shape is what both sides agree on.
 *
 * @param altitude metres, optional (null = not reported)
 * @param heading  degrees, 0 ≤ heading &lt; 360, optional
 */
public record TrackUpdateMessage(
        String trackId,
        Instant timestamp,
        double latitude,
        double longitude,
        Double altitude,
        Double heading,
        Identity identity) {

    TrackUpdateMessage withLatitude(double newLatitude) {
        return new TrackUpdateMessage(trackId, timestamp, newLatitude, longitude, altitude, heading, identity);
    }
}
