package dev.kostas.trackstore.picture;

import dev.kostas.trackstore.track.TrackNotFoundException;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read API for the current picture: the latest known state of every track.
 */
@RestController
public class PictureController {

    private final TrackStateRepository picture;

    public PictureController(TrackStateRepository picture) {
        this.picture = picture;
    }

    /** All tracks, ordered by {@code trackId} so the response is stable between calls. */
    @GetMapping("/picture")
    public List<TrackStateResponse> picture() {
        return picture.findAll(Sort.by("trackId")).stream()
                .map(TrackStateResponse::from)
                .toList();
    }

    /** The latest state of one track; {@code 404} if the track is unknown. */
    @GetMapping("/tracks/{trackId}")
    public TrackStateResponse track(@PathVariable String trackId) {
        return picture.findById(trackId)
                .map(TrackStateResponse::from)
                .orElseThrow(() -> new TrackNotFoundException(trackId));
    }
}
