package dev.kostas.trackstore;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Real infrastructure for tests, started in Docker by Testcontainers.
 * <p>
 * {@link ServiceConnection} makes Spring Boot point the DataSource at the container
 * automatically — no URLs or ports in test properties. Test classes {@code @Import} this
 * configuration; Spring's test-context cache then shares one container across them.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        // Same major version as compose.yaml, so tests run against what we deploy.
        return new PostgreSQLContainer("postgres:17");
    }
}
