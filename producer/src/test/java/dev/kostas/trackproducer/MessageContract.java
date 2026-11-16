package dev.kostas.trackproducer;

/**
 * The validation rules of the track-updates contract (see the requirements), checked by hand: the
 * producer doesn't share the service's Bean Validation annotations, it just has to respect the rules.
 */
final class MessageContract {

    private MessageContract() {
    }

    static boolean isValid(TrackUpdateMessage m) {
        return m.trackId() != null && !m.trackId().isBlank()
                && m.timestamp() != null
                && m.latitude() >= -90 && m.latitude() <= 90
                && m.longitude() >= -180 && m.longitude() <= 180
                && (m.altitude() == null || m.altitude() >= 0)
                && (m.heading() == null || (m.heading() >= 0 && m.heading() < 360))
                && m.identity() != null;
    }
}
