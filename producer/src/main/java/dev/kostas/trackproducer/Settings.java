package dev.kostas.trackproducer;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Producer settings. Each one can be given as a command-line option ({@code --ticks=60}) or as an
 * environment variable ({@code PRODUCER_TICKS=60}); the command line wins, then the environment,
 * then the default.
 *
 * @param ticks how many rounds of updates to publish; {@code 0} = until stopped (Ctrl+C)
 */
record Settings(String bootstrapServers, String topic, int tracks, int ticks, Duration interval) {

    private static final Map<String, String> DEFAULTS = defaults();
    private static final Pattern SIMPLE_DURATION = Pattern.compile("(\\d+)(ms|s|m)");

    private static Map<String, String> defaults() {
        Map<String, String> defaults = new LinkedHashMap<>();
        defaults.put("bootstrap-servers", "localhost:9092");
        defaults.put("topic", "track-updates");
        defaults.put("tracks", "3");
        defaults.put("ticks", "30");
        defaults.put("interval", "1s");
        return defaults;
    }

    Settings {
        if (tracks < 1 || tracks > TrackSimulator.MAX_TRACKS) {
            throw new IllegalArgumentException("tracks must be between 1 and " + TrackSimulator.MAX_TRACKS);
        }
        if (ticks < 0 || (ticks > 0 && ticks < DemoScenario.MIN_TICKS)) {
            throw new IllegalArgumentException("ticks must be 0 (run until stopped) or at least "
                    + DemoScenario.MIN_TICKS + " (so that every anomaly is sent)");
        }
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException("interval must be positive");
        }
    }

    boolean runsUntilStopped() {
        return ticks == 0;
    }

    /**
     * @throws IllegalArgumentException with a message fit for the user, for an unknown option or a bad value
     */
    static Settings parse(String[] args, Map<String, String> environment) {
        Map<String, String> values = new LinkedHashMap<>(DEFAULTS);

        for (String name : DEFAULTS.keySet()) {
            String fromEnvironment = environment.get(environmentVariable(name));
            if (fromEnvironment != null && !fromEnvironment.isBlank()) {
                values.put(name, fromEnvironment.strip());
            }
        }
        for (String arg : args) {
            int equals = arg.indexOf('=');
            if (!arg.startsWith("--") || equals < 0) {
                throw new IllegalArgumentException("expected --name=value, got '" + arg + "'");
            }
            String name = arg.substring(2, equals);
            if (!DEFAULTS.containsKey(name)) {
                throw new IllegalArgumentException("unknown option --" + name);
            }
            values.put(name, arg.substring(equals + 1));
        }

        return new Settings(
                values.get("bootstrap-servers"),
                values.get("topic"),
                integer(values, "tracks"),
                integer(values, "ticks"),
                duration(values.get("interval")));
    }

    static String usage() {
        StringBuilder usage = new StringBuilder("""
                Publishes simulated track updates to Kafka, including a duplicate, an out-of-order
                and two invalid messages.

                Usage: java -jar track-producer.jar [--name=value ...]

                Options (or environment variables), with defaults:
                """);
        DEFAULTS.forEach((name, value) -> usage.append("  --%-18s %-28s %s%n"
                .formatted(name, environmentVariable(name), value)));
        usage.append("""

                ticks: 0 = run until stopped (Ctrl+C). interval: e.g. 500ms, 2s, 1m or ISO-8601 (PT1S).
                """);
        return usage.toString();
    }

    private static String environmentVariable(String name) {
        return "PRODUCER_" + name.toUpperCase().replace('-', '_');
    }

    private static int integer(Map<String, String> values, String name) {
        try {
            return Integer.parseInt(values.get(name));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a whole number, got '" + values.get(name) + "'");
        }
    }

    static Duration duration(String text) {
        Matcher simple = SIMPLE_DURATION.matcher(text);
        if (simple.matches()) {
            long amount = Long.parseLong(simple.group(1));
            return switch (simple.group(2)) {
                case "ms" -> Duration.ofMillis(amount);
                case "s" -> Duration.ofSeconds(amount);
                default -> Duration.ofMinutes(amount);
            };
        }
        try {
            return Duration.parse(text);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("interval must look like 500ms, 2s, 1m or PT1S, got '" + text + "'");
        }
    }
}
