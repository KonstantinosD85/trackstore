package dev.kostas.trackproducer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes a {@link DemoScenario} tick by tick: each tick's messages, then a pause of one interval.
 * <p>
 * Takes the Kafka {@link Producer} interface rather than creating one, so tests can pass Kafka's
 * {@code MockProducer} (no broker) or a real producer.
 */
final class TrackFeedPublisher {

    private static final Logger log = LoggerFactory.getLogger(TrackFeedPublisher.class);
    private static final long SEND_TIMEOUT_SECONDS = 30;

    private final Producer<String, String> producer;
    private final String topic;

    TrackFeedPublisher(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    /**
     * @param ticks how many ticks to publish; 0 = until {@code stop} is released
     * @param stop  released to stop (e.g. on Ctrl+C); the pause between ticks waits on it, so a stop
     *              ends the run promptly, but never in the middle of a tick
     * @return the number of ticks published
     */
    int publish(DemoScenario scenario, int ticks, Duration interval, CountDownLatch stop)
            throws InterruptedException, ExecutionException, TimeoutException {
        int published = 0;
        while ((ticks == 0 || published < ticks) && stop.getCount() > 0) {
            if (published > 0 && stop.await(interval.toNanos(), TimeUnit.NANOSECONDS)) {
                break;   // stop requested during the pause
            }
            List<DemoScenario.Message> messages = scenario.nextTick();
            for (DemoScenario.Message message : messages) {
                // Wait for the broker's acknowledgement: strictly in order, and errors surface here.
                producer.send(new ProducerRecord<>(topic, message.key(), message.payload()))
                        .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (message.kind() != DemoScenario.Kind.UPDATE) {
                    log.info("tick {}: sent {} for {} — {}", published, message.kind(), message.key(), message.note());
                }
            }
            log.info("tick {}: {} message(s) sent", published, messages.size());
            published++;
        }
        return published;
    }
}
