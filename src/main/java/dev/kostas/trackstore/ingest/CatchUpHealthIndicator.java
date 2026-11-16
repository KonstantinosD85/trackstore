package dev.kostas.trackstore.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Readiness after a (re)start, shown as {@code catchUp} and part of the {@code readiness} health
 * group ({@code /health/readiness}).
 * <p>
 * After a restart the consumer resumes from its committed offset and first works through what was
 * published while it was down. Until then the picture is out of date, so the instance reports
 * {@code OUT_OF_SERVICE} — "don't send me traffic yet", which a load balancer or Kubernetes
 * readiness probe respects without restarting anything.
 * <p>
 * Once the lag has dropped to {@code trackstore.ingestion.ready-max-lag}, the instance is
 * <b>caught up for good</b> (the state latches). A later burst of lag must not mark every instance
 * not-ready at the same time and take the API offline; lag stays visible in {@code consumerLag}.
 * <p>
 * Evaluated when health is requested (probes poll it regularly), not in the background.
 */
@Component
class CatchUpHealthIndicator implements HealthIndicator {

    private static final Logger log = LoggerFactory.getLogger(CatchUpHealthIndicator.class);

    private final ConsumerLag lag;
    private final long readyMaxLag;
    private volatile boolean caughtUp;

    CatchUpHealthIndicator(ConsumerLag lag, @Value("${trackstore.ingestion.ready-max-lag}") long readyMaxLag) {
        this.lag = lag;
        this.readyMaxLag = readyMaxLag;
    }

    @Override
    public Health health() {
        if (caughtUp) {
            return Health.up().withDetail("caughtUp", true).build();
        }

        long totalLag;
        try {
            totalLag = ConsumerLag.total(lag.byPartition());
        } catch (Exception e) {
            return Health.outOfService().withException(e).withDetail("caughtUp", false).build();
        }

        if (totalLag <= readyMaxLag) {
            caughtUp = true;
            log.info("Caught up with {} (lag {} ≤ {}): ready for traffic", Topics.TRACK_UPDATES, totalLag, readyMaxLag);
            return Health.up().withDetail("caughtUp", true).build();
        }
        return Health.outOfService()
                .withDetail("caughtUp", false)
                .withDetail("totalLag", totalLag)
                .withDetail("readyAtLag", readyMaxLag)
                .build();
    }
}
