package dev.kostas.trackproducer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * Test clock: the first read returns {@code start}, each further read one {@code step} later.
 * {@link DemoScenario} reads the clock once per tick, so tick {@code n} is at {@code start + n × step}.
 */
final class SteppingClock extends Clock {

    private final Duration step;
    private Instant next;

    SteppingClock(Instant start, Duration step) {
        this.next = start;
        this.step = step;
    }

    @Override
    public Instant instant() {
        Instant now = next;
        next = next.plus(step);
        return now;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        throw new UnsupportedOperationException("not needed in tests");
    }
}
