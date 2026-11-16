package dev.kostas.trackstore.track;

/**
 * Domain error: the requested track has never been seen (or was dropped from the picture).
 * <p>
 * Thrown by the read services. It knows nothing about HTTP; the web layer
 * ({@code web.ApiExceptionHandler}) decides that it becomes a {@code 404}.
 */
public class TrackNotFoundException extends RuntimeException {

    private final String trackId;

    public TrackNotFoundException(String trackId) {
        super("No track with id '" + trackId + "'");
        this.trackId = trackId;
    }

    public String trackId() {
        return trackId;
    }
}
