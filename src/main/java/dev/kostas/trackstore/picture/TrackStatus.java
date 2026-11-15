package dev.kostas.trackstore.picture;

/**
 * Liveness of a track in the picture, based on the age of its latest observation
 * (see {@link PictureExpiryProperties}). A track past {@code drop-after} is not a status:
 * it is removed from the picture (its history is kept).
 */
public enum TrackStatus {

    /** Observed within {@code stale-after}. */
    ACTIVE,

    /** No observation for longer than {@code stale-after}; the shown position is getting old. */
    STALE
}
