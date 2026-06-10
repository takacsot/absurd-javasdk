package io.absurd.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Defines the backoff strategy between task retry attempts.
 *
 * @param kind        the retry algorithm: FIXED (constant delay), EXPONENTIAL (growing delay),
 *                    or NONE (immediate retry)
 * @param baseSeconds the initial delay in seconds before the first retry. For FIXED, this is
 *                    the constant delay. For EXPONENTIAL, this is the starting delay that grows
 *                    by the factor on each attempt
 * @param factor      multiplier applied to the delay after each attempt (EXPONENTIAL only).
 *                    E.g., base=1, factor=2 yields delays of 1s, 2s, 4s, 8s...
 * @param maxSeconds  upper bound on the delay in seconds (EXPONENTIAL only); prevents
 *                    unbounded growth. E.g., with maxSeconds=60, delays never exceed 60s
 */
/**
 * Defines the backoff strategy between task retry attempts.
 *
 * @param kind        the retry algorithm: FIXED (constant delay), EXPONENTIAL (growing delay),
 *                    or NONE (immediate retry)
 * @param baseSeconds the initial delay in seconds before the first retry. For FIXED, this is
 *                    the constant delay. For EXPONENTIAL, this is the starting delay that grows
 *                    by the factor on each attempt
 * @param factor      multiplier applied to the delay after each attempt (EXPONENTIAL only).
 *                    E.g., base=1, factor=2 yields delays of 1s, 2s, 4s, 8s...
 * @param maxSeconds  upper bound on the delay in seconds (EXPONENTIAL only); prevents
 *                    unbounded growth. E.g., with maxSeconds=60, delays never exceed 60s
 */
public record RetryStrategy(Kind kind, Double baseSeconds, Double factor, Double maxSeconds) {

    public enum Kind {
        FIXED("fixed"),
        EXPONENTIAL("exponential"),
        NONE("none");

        private final String value;

        Kind(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }

    public static RetryStrategy fixed(double baseSeconds) {
        return new RetryStrategy(Kind.FIXED, baseSeconds, null, null);
    }

    public static RetryStrategy exponential(double baseSeconds, double factor, double maxSeconds) {
        return new RetryStrategy(Kind.EXPONENTIAL, baseSeconds, factor, maxSeconds);
    }

    public static RetryStrategy none() {
        return new RetryStrategy(Kind.NONE, null, null, null);
    }

    ObjectNode toJson() {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var node = mapper.createObjectNode();
        node.put("kind", kind.value());
        if (baseSeconds != null) {
            node.put("base_seconds", baseSeconds);
        }
        if (factor != null) {
            node.put("factor", factor);
        }
        if (maxSeconds != null) {
            node.put("max_seconds", maxSeconds);
        }
        return node;
    }
}
