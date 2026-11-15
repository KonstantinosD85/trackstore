package dev.kostas.trackstore.producer;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Settings of the demo producer, bound from {@code trackstore.demo-producer.*}
 * (see {@code application-producer.yaml}).
 * <p>
 * {@link Validated} makes Spring Boot check the constraints when binding: a bad value fails the
 * startup with a clear message instead of misbehaving later.
 *
 * @param tracks   how many tracks to simulate
 * @param ticks    how many rounds of updates to publish (at least {@value DemoScenario#MIN_TICKS},
 *                 so that every anomaly happens)
 * @param interval time between rounds, both in wall-clock time and in the updates' timestamps
 */
@ConfigurationProperties("trackstore.demo-producer")
@Validated
public record DemoProducerProperties(
        @Min(1) @Max(10) int tracks,
        @Min(DemoScenario.MIN_TICKS) int ticks,
        @NotNull Duration interval) {
}
