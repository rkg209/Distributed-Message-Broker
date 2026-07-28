package io.minikafka.bench;

import java.time.Duration;

/**
 * Parsed {@code --flag=value} command-line arguments for {@link LoadGenerator}. Every value is
 * optional: an unset field falls back to {@link BenchConfig}'s system-property-driven defaults, so
 * {@code LoadGenerator} can be pointed at without any flags at all for its default 5-minute soak.
 */
public record LoadGeneratorArgs(
    long messages,
    Integer payloadSize,
    Integer threads,
    Integer rf,
    Duration duration,
    String bootstrapHost,
    Integer bootstrapPort,
    String topic,
    Integer partitions) {

  private static final Duration DEFAULT_DURATION = Duration.ofMinutes(5);

  public static LoadGeneratorArgs parse(String[] args) {
    long messages = 0;
    Integer payloadSize = null;
    Integer threads = null;
    Integer rf = null;
    Duration duration = DEFAULT_DURATION;
    String bootstrapHost = null;
    Integer bootstrapPort = null;
    String topic = null;
    Integer partitions = null;

    for (String arg : args) {
      String flag;
      String value;
      int eq = arg.indexOf('=');
      if (arg.startsWith("--") && eq > 0) {
        flag = arg.substring(2, eq);
        value = arg.substring(eq + 1);
      } else {
        throw new IllegalArgumentException("Malformed argument (expected --flag=value): " + arg);
      }
      switch (flag) {
        case "messages" -> messages = parseLong(flag, value);
        case "payload-size" -> payloadSize = parseInt(flag, value);
        case "threads" -> threads = parseInt(flag, value);
        case "rf" -> rf = parseInt(flag, value);
        case "duration" -> duration = parseDuration(value);
        case "bootstrap" -> {
          int colon = value.lastIndexOf(':');
          if (colon <= 0 || colon == value.length() - 1) {
            throw new IllegalArgumentException(
                "Malformed --bootstrap (expected host:port): " + value);
          }
          bootstrapHost = value.substring(0, colon);
          bootstrapPort = parseInt("bootstrap port", value.substring(colon + 1));
        }
        case "topic" -> topic = value;
        case "partitions" -> partitions = parseInt(flag, value);
        default -> throw new IllegalArgumentException("Unknown argument: --" + flag);
      }
    }

    return new LoadGeneratorArgs(
        messages,
        payloadSize,
        threads,
        rf,
        duration,
        bootstrapHost,
        bootstrapPort,
        topic,
        partitions);
  }

  private static Duration parseDuration(String value) {
    if (value.isEmpty()) {
      throw new IllegalArgumentException("--duration must not be empty");
    }
    char unit = value.charAt(value.length() - 1);
    String numberPart = Character.isDigit(unit) ? value : value.substring(0, value.length() - 1);
    long amount = parseLong("duration", numberPart);
    return switch (Character.isDigit(unit) ? 's' : Character.toLowerCase(unit)) {
      case 's' -> Duration.ofSeconds(amount);
      case 'm' -> Duration.ofMinutes(amount);
      case 'h' -> Duration.ofHours(amount);
      default ->
          throw new IllegalArgumentException(
              "Malformed --duration (expected a number optionally suffixed with s/m/h): " + value);
    };
  }

  private static int parseInt(String flag, String value) {
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Malformed --" + flag + ": " + value, e);
    }
  }

  private static long parseLong(String flag, String value) {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Malformed --" + flag + ": " + value, e);
    }
  }
}
