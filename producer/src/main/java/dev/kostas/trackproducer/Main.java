package dev.kostas.trackproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Entry point of the demo track feed.
 * <p>
 * Exit codes: {@code 0} all ticks published, {@code 1} Kafka failure (e.g. broker unreachable,
 * topic missing), {@code 2} invalid options. Stopped with Ctrl+C / SIGTERM: the current tick is
 * finished and the producer flushed and closed; the exit code is then the usual one for a signal
 * (130 / 143), since the JVM is already shutting down.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 1 && (args[0].equals("help") || args[0].equals("--help"))) {
            System.out.print(Settings.usage());
            return;
        }

        Settings settings;
        try {
            settings = Settings.parse(args, System.getenv());
        } catch (IllegalArgumentException e) {
            System.err.println("Invalid option: " + e.getMessage() + "\n");
            System.err.print(Settings.usage());
            System.exit(2);
            return;
        }

        System.exit(run(settings));
    }

    /** Publishes until done or stopped; returns the exit code. */
    static int run(Settings settings) {
        log.info("Publishing {} track(s) to {} on {}, every {}, {}",
                settings.tracks(), settings.topic(), settings.bootstrapServers(), settings.interval(),
                settings.runsUntilStopped() ? "until stopped (Ctrl+C)" : settings.ticks() + " ticks");

        // Ctrl+C runs shutdown hooks while the main thread keeps going: the hook releases `stop`
        // (the publisher's pause waits on it), then waits until the producer has been flushed
        // and closed. No thread interrupts — they would make Kafka's close() fail.
        CountDownLatch stop = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stop.countDown();
            awaitQuietly(closed);
        }, "shutdown"));

        DemoScenario scenario = new DemoScenario(new TrackSimulator(settings.tracks()), JsonMapper.builder().build(),
                Instant.now().truncatedTo(ChronoUnit.SECONDS), settings.interval());

        try (Producer<String, String> producer = createProducer(settings.bootstrapServers())) {
            int ticks = new TrackFeedPublisher(producer, settings.topic())
                    .publish(scenario, settings.ticks(), settings.interval(), stop);
            log.info("{}: {} tick(s) published", stop.getCount() == 0 ? "Stopped" : "Done", ticks);
            return 0;
        } catch (Exception e) {
            log.error("Publishing failed: {}", e.getMessage());
            return 1;
        } finally {
            closed.countDown();
        }
    }

    static Producer<String, String> createProducer(String bootstrapServers) {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, "track-producer",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                // Wait for the write to be committed, and let the broker discard retried duplicates.
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                // Fail within 10 s (instead of 60) if the broker or topic is missing — the broker
                // doesn't create topics automatically (see compose.yaml).
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 10_000));
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
