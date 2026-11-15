package dev.kostas.trackstore.picture;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

/**
 * Wiring for the picture's expiry: binds {@link PictureExpiryProperties}, turns on
 * {@code @Scheduled} methods, and provides the {@link Clock}.
 * <p>
 * Code asks the injected clock for "now" instead of calling {@code Instant.now()}, so tests can
 * replace it with a fixed clock and control time.
 */
@Configuration
@EnableScheduling
@EnableConfigurationProperties(PictureExpiryProperties.class)
class PictureConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
