package dev.kostas.trackstore.producer;

import dev.kostas.trackstore.track.TrackUpdate;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The messages the demo producer publishes, tick by tick: every track's position at each tick,
 * plus the anomalies the requirements ask us to demonstrate.
 * <ul>
 *   <li>tick {@value #DUPLICATE_TICK}: one update is sent twice (like a producer retry) → DUPLICATE</li>
 *   <li>tick {@value #OUT_OF_ORDER_TICK}: one track's update from the previous tick is held back and
 *       sent after its newer one → STALE (kept in history, picture not overwritten)</li>
 *   <li>tick {@value #INVALID_TICK}: one message with an invalid value and one that cannot be parsed
 *       → rejected (log line + dead-letter topic), stored nowhere</li>
 * </ul>
 * Pure: builds the messages without sending them, so the scenario can be unit-tested.
 */
final class DemoScenario {

    static final int DUPLICATE_TICK = 3;
    static final int OUT_OF_ORDER_TICK = 5;
    static final int INVALID_TICK = 7;
    /** Enough ticks for every anomaly to happen. */
    static final int MIN_TICKS = INVALID_TICK + 1;

    enum Kind { UPDATE, DUPLICATE, OUT_OF_ORDER, INVALID }

    /** One Kafka record to publish: key = trackId, value = JSON text. */
    record Message(String key, String payload, Kind kind, String note) {
    }

    private final TrackSimulator simulator;
    private final JsonMapper json;

    DemoScenario(TrackSimulator simulator, JsonMapper json) {
        this.simulator = simulator;
        this.json = json;
    }

    /** The messages of every tick; tick {@code n} observes the tracks at {@code start + n * interval}. */
    List<List<Message>> build(Instant start, Duration interval, int ticks) {
        if (ticks < MIN_TICKS) {
            throw new IllegalArgumentException("need at least " + MIN_TICKS + " ticks, got " + ticks);
        }
        int trackCount = simulator.tracks().size();
        int duplicated = 0;
        int delayed = 1 % trackCount;
        int invalid = 2 % trackCount;

        List<List<Message>> scenario = new ArrayList<>();
        Message heldBack = null;

        for (int tick = 0; tick < ticks; tick++) {
            List<Message> messages = new ArrayList<>();
            List<TrackUpdate> updates = simulator.observe(start, interval.multipliedBy(tick));

            for (int track = 0; track < trackCount; track++) {
                Message message = update(updates.get(track));

                if (tick == OUT_OF_ORDER_TICK - 1 && track == delayed) {
                    heldBack = message;   // published one tick late, see below
                    continue;
                }
                messages.add(message);

                if (tick == DUPLICATE_TICK && track == duplicated) {
                    messages.add(new Message(message.key(), message.payload(), Kind.DUPLICATE,
                            "same message sent twice"));
                }
                if (tick == OUT_OF_ORDER_TICK && track == delayed) {
                    messages.add(new Message(heldBack.key(), heldBack.payload(), Kind.OUT_OF_ORDER,
                            "update of tick " + (tick - 1) + " sent after tick " + tick));
                }
            }

            if (tick == INVALID_TICK) {
                TrackUpdate source = updates.get(invalid);
                messages.add(invalid(withLatitude(source, 91.0), "latitude 91 (out of range)"));
                messages.add(invalid(json.writeValueAsString(source).replace(
                                "\"identity\":\"" + source.identity() + "\"", "\"identity\":\"ENEMY\""),
                        source.trackId(), "identity ENEMY (not a known value)"));
            }
            scenario.add(messages);
        }
        return scenario;
    }

    private Message update(TrackUpdate update) {
        return new Message(update.trackId(), json.writeValueAsString(update), Kind.UPDATE, null);
    }

    private Message invalid(TrackUpdate update, String note) {
        return invalid(json.writeValueAsString(update), update.trackId(), note);
    }

    private static Message invalid(String payload, String key, String note) {
        return new Message(key, payload, Kind.INVALID, note);
    }

    private static TrackUpdate withLatitude(TrackUpdate u, double latitude) {
        return new TrackUpdate(u.trackId(), u.timestamp(), latitude, u.longitude(), u.altitude(), u.heading(), u.identity());
    }
}
