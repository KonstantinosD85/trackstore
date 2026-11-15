package dev.kostas.trackstore.ingest;

/**
 * What processing a valid track update did to history and the picture.
 */
public enum ProcessingOutcome {

    /** New observation and the latest for its track: appended to history and shown in the picture. */
    APPLIED,

    /** New observation, but older than the track's current state (arrived out of order), or
     *  already older than {@code drop-after}: appended to history, picture unchanged. */
    STALE,

    /** Already processed (redelivery or replay): neither history nor picture changed. */
    DUPLICATE
}
