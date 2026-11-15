package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.track.Identity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * The latest known state of one track — one row of the picture.
 * <p>
 * Rows are written only through {@link TrackStateRepository#upsertIfNewer}, which enforces
 * "newer timestamp wins" inside the database, so this entity exposes getters only.
 */
@Entity
@Table(name = "track_picture")
public class TrackState {

    @Id
    @Column(name = "track_id", length = 64)
    private String trackId;

    @Column(name = "observed_at", nullable = false)
    private Instant observedAt;

    @Column(nullable = false)
    private Double latitude;

    @Column(nullable = false)
    private Double longitude;

    private Double altitude;

    private Double heading;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private Identity identity;

    @Column(name = "last_received_at", nullable = false, insertable = false, updatable = false)
    private Instant lastReceivedAt;

    protected TrackState() {
    }

    public String getTrackId() {
        return trackId;
    }

    public Instant getObservedAt() {
        return observedAt;
    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public Double getAltitude() {
        return altitude;
    }

    public Double getHeading() {
        return heading;
    }

    public Identity getIdentity() {
        return identity;
    }

    public Instant getLastReceivedAt() {
        return lastReceivedAt;
    }
}
