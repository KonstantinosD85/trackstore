package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.track.TrackNotFoundException;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Read side of the picture. Business answers ("this track doesn't exist") are domain exceptions;
 * turning them into HTTP is the web layer's job.
 */
@Service
@Transactional(readOnly = true)
public class PictureQueryService {

    private final TrackStateRepository picture;

    public PictureQueryService(TrackStateRepository picture) {
        this.picture = picture;
    }

    /** All tracks, ordered by {@code trackId} so the result is stable between calls. */
    public List<TrackState> allTracks() {
        return picture.findAll(Sort.by("trackId"));
    }

    /** The latest state of one track. */
    public TrackState track(String trackId) {
        return picture.findById(trackId).orElseThrow(() -> new TrackNotFoundException(trackId));
    }
}
