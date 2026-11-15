package dev.kostas.trackstore.ingest;

/**
 * Kafka topic names.
 */
public final class Topics {

    /** Track updates from external sources (sensors, data links), keyed by {@code trackId}. */
    public static final String TRACK_UPDATES = "track-updates";

    /** Rejected track updates (unparseable or invalid), with the original payload unchanged. */
    public static final String TRACK_UPDATES_DLT = "track-updates-dlt";

    private Topics() {
    }
}
