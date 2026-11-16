package dev.kostas.trackstore.ingest;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistrar;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJacksonJsonMessageConverter;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;

/**
 * Kafka wiring for track-update ingestion. Spring Boot's auto-configured listener container
 * factory picks up the {@link RecordMessageConverter} and {@link DefaultErrorHandler} beans
 * defined here.
 */
@Configuration
public class KafkaConfig implements KafkaListenerConfigurer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    private static final int PARTITIONS = 3;

    private final LocalValidatorFactoryBean validator;

    public KafkaConfig(LocalValidatorFactoryBean validator) {
        this.validator = validator;
    }

    /**
     * Enables {@code @Valid} on {@code @KafkaListener} payloads, using Spring Boot's
     * auto-configured Bean Validation validator.
     */
    @Override
    public void configureKafkaListeners(KafkaListenerEndpointRegistrar registrar) {
        registrar.setValidator(validator);
    }

    /**
     * Converts the JSON record value (consumed as a String) into the listener's parameter type,
     * using Spring Boot's JSON mapper. Because conversion happens here and not in the Kafka
     * deserializer, the record keeps the original text — which is what the DLT receives.
     */
    @Bean
    RecordMessageConverter trackUpdateMessageConverter(JsonMapper jsonMapper) {
        return new StringJacksonJsonMessageConverter(jsonMapper);
    }

    /**
     * Failure handling for the listener:
     * <ul>
     *   <li><b>Invalid input</b> (conversion or {@code @Valid} failure): Spring Kafka classifies these
     *       as not retryable, so they go straight to the recoverer → log line + DLT.</li>
     *   <li><b>Anything else</b> (e.g. database down): retried with exponential back-off, without a
     *       limit. A valid update is never dropped, and because the partition waits, later updates
     *       of the same track are not processed before it (ordering is preserved).</li>
     * </ul>
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaOperations<?, ?> kafkaTemplate) {
        DeadLetterPublishingRecoverer deadLetter = new DeadLetterPublishingRecoverer(kafkaTemplate,
                // Partition -1: let Kafka choose by key, so the DLT needn't match the source's partitions.
                (record, exception) -> new TopicPartition(Topics.TRACK_UPDATES_DLT, -1));

        ConsumerRecordRecoverer logAndDeadLetter = (record, exception) -> {
            log.warn("Rejected track update [topic={}, partition={}, offset={}, key={}]: {}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    RejectionReason.describe(exception));
            deadLetter.accept(record, exception);
        };

        ExponentialBackOff backOff = new ExponentialBackOff(Duration.ofMillis(500).toMillis(), 2.0);
        backOff.setMaxInterval(Duration.ofSeconds(30).toMillis());

        return new DefaultErrorHandler(logAndDeadLetter, backOff);
    }

    /**
     * Kafka admin client for {@link ConsumerLagHealthIndicator}, built from Spring Boot's admin
     * settings (same bootstrap servers). Long-lived, and closed by Spring on shutdown
     * ({@code close()} is detected as the destroy method).
     */
    @Bean
    Admin kafkaAdminClient(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }

    // Topic provisioning for local development. In production the source topic belongs to the
    // platform/producers; we would only own (and create) the dead-letter topic.

    @Bean
    NewTopic trackUpdatesTopic() {
        return TopicBuilder.name(Topics.TRACK_UPDATES).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic trackUpdatesDeadLetterTopic() {
        return TopicBuilder.name(Topics.TRACK_UPDATES_DLT).partitions(PARTITIONS).replicas(1).build();
    }
}
