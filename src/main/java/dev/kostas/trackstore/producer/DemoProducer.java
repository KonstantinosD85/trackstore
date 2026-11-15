package dev.kostas.trackstore.producer;

import dev.kostas.trackstore.ingest.Topics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Demo producer: publishes {@link DemoScenario} to {@value Topics#TRACK_UPDATES} once the
 * application has started, playing the external sensor feed.
 * <p>
 * Exists only with the {@code producer} profile ({@code @Profile}), so a normal run of the service
 * never publishes anything. It reaches Kafka only through {@link KafkaTemplate} and sends raw JSON
 * text, exactly like an external producer — it does not call the ingestion code directly.
 */
@Component
@Profile("producer")
@EnableConfigurationProperties(DemoProducerProperties.class)
class DemoProducer implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoProducer.class);

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final DemoProducerProperties properties;

    DemoProducer(KafkaTemplate<String, String> kafka, JsonMapper json, DemoProducerProperties properties) {
        this.kafka = kafka;
        this.json = json;
        this.properties = properties;
    }

    @Override
    public void run(String... args) throws InterruptedException {
        DemoScenario scenario = new DemoScenario(new TrackSimulator(properties.tracks()), json);
        Instant start = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        List<List<DemoScenario.Message>> ticks = scenario.build(start, properties.interval(), properties.ticks());

        log.info("Demo producer: {} tracks, {} ticks every {} → topic {}",
                properties.tracks(), properties.ticks(), properties.interval(), Topics.TRACK_UPDATES);

        for (int tick = 0; tick < ticks.size(); tick++) {
            for (DemoScenario.Message message : ticks.get(tick)) {
                // Wait for the broker's ack: keeps the demo strictly in order and surfaces errors here.
                kafka.send(Topics.TRACK_UPDATES, message.key(), message.payload()).join();
                if (message.kind() != DemoScenario.Kind.UPDATE) {
                    log.info("Demo producer tick {}: sent {} for {} — {}",
                            tick, message.kind(), message.key(), message.note());
                }
            }
            Thread.sleep(properties.interval());
        }
        log.info("Demo producer finished: {} ticks sent. Try GET /picture", ticks.size());
    }
}
