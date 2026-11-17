package dev.kostas.trackproducer;

import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * The messages of the demo feed, tick by tick: every track's position at each tick, plus the
 * anomalies the service must handle.
 * <ul>
 *   <li>tick {@value #DUPLICATE_TICK}: one update is sent twice (like a producer retry) → DUPLICATE</li>
 *   <li>tick {@value #OUT_OF_ORDER_TICK}: one track's update from the previous tick is held back and
 *       sent after its newer one → STALE (kept in history, picture not overwritten)</li>
 *   <li>tick {@value #INVALID_TICK}: one message with an invalid value and one that cannot be parsed
 *       → rejected by the service (log line + dead-letter topic), stored nowhere</li>
 * </ul>
 * Each anomaly happens once; after that the tracks just keep moving, for as many ticks as asked.
 * <p>
 * Timestamps come from the {@link Clock}, read when a tick is built, like a sensor stamping the
 * moment it observes. (Computing them as {@code start + tick × interval} drifted behind real time by
 * the time each tick took to send, ~12 ms per tick: after an hour, "live" positions were ~40 s old
 * and the service dropped them as expired.)
 * <p>
 * Builds messages without sending them, so the scenario can be unit-tested. Stateful (the current
 * tick, the held-back update, the start time): use one instance per run.
 */
final class DemoScenario {

    static final int DUPLICATE_TICK = 3;
    static final int OUT_OF_ORDER_TICK = 5;
    static final int INVALID_TICK = 7;
    /** A finite run needs this many ticks for every anomaly to happen. */
    static final int MIN_TICKS = INVALID_TICK + 1;

    enum Kind { UPDATE, DUPLICATE, OUT_OF_ORDER, INVALID }

    /** One Kafka record to publish: key = trackId, value = JSON text. */
    record Message(String key, String payload, Kind kind, String note) {
    }

    private final TrackSimulator simulator;
    private final JsonMapper json;
    private final Clock clock;

    private int tick = 0;
    private Instant start;   // time of the first tick; positions are computed from it
    private Message heldBack;

    DemoScenario(TrackSimulator simulator, JsonMapper json, Clock clock) {
        this.simulator = simulator;
        this.json = json;
        this.clock = clock;
    }

    /** The messages of the next tick, in publishing order. */
    List<Message> nextTick() {
        int trackCount = simulator.tracks().size();
        int duplicated = 0;
        int delayed = 1 % trackCount;
        int invalid = 2 % trackCount;

        List<Message> messages = new ArrayList<>();
        // Milliseconds, like a typical sensor; one reading per tick, so all tracks share it.
        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        if (start == null) {
            start = now;
        }
        List<TrackUpdateMessage> updates = simulator.observe(start, Duration.between(start, now));

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
            TrackUpdateMessage source = updates.get(invalid);
            messages.add(new Message(source.trackId(), json.writeValueAsString(source.withLatitude(91.0)),
                    Kind.INVALID, "latitude 91 (out of range)"));
            messages.add(new Message(source.trackId(), json.writeValueAsString(source).replace(
                            "\"identity\":\"" + source.identity() + "\"", "\"identity\":\"ENEMY\""),
                    Kind.INVALID, "identity ENEMY (not a known value)"));
        }

        tick++;
        return messages;
    }

    private Message update(TrackUpdateMessage update) {
        return new Message(update.trackId(), json.writeValueAsString(update), Kind.UPDATE, null);
    }
}
