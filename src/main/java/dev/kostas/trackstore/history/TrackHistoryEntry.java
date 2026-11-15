package dev.kostas.trackstore.history;

import dev.kostas.trackstore.track.Identity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * One accepted track update, as stored in the history log.
 * <p>
 * History is append-only: rows are inserted through
 * {@link TrackHistoryRepository#appendIfAbsent} and never modified, so this entity
 * exposes getters only.
 */
@Entity
@Table(name = "track_history")
public class TrackHistoryEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "track_id", nullable = false, length = 64)
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

    @Column(name = "received_at", nullable = false, insertable = false, updatable = false)
    private Instant receivedAt;

    /** Required by JPA; not for application use. */
    protected TrackHistoryEntry() {
    }

    public Long getId() {
        return id;
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

    public Instant getReceivedAt() {
        return receivedAt;
    }
}
