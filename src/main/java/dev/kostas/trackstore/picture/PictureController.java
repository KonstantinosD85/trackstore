package dev.kostas.trackstore.picture;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Read API for the current picture: the latest known state of every track.
 * Errors (unknown track, database down) are mapped to HTTP by {@code web.ApiExceptionHandler}.
 */
@RestController
public class PictureController {

    private final PictureQueryService picture;

    public PictureController(PictureQueryService picture) {
        this.picture = picture;
    }

    /** All tracks, ordered by {@code trackId}. */
    @GetMapping("/picture")
    public List<TrackStateResponse> picture() {
        return picture.allTracks().stream()
                .map(TrackStateResponse::from)
                .toList();
    }

    /** The latest state of one track; {@code 404} if the track is unknown. */
    @GetMapping("/tracks/{trackId}")
    public TrackStateResponse track(@PathVariable String trackId) {
        return TrackStateResponse.from(picture.track(trackId));
    }
}
