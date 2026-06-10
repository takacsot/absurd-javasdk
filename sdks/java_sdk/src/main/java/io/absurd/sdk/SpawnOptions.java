package io.absurd.sdk;

import java.util.Map;

/**
 * Configuration options for spawning a task via {@link Absurd#spawn}.
 *
 * @param maxAttempts    maximum number of execution attempts before permanent failure;
 *                       overrides the task registration default and client default.
 *                       {@code null} means use the registration or client default (typically 5)
 * @param retryStrategy  backoff strategy between retry attempts; determines how long to wait
 *                       before the next attempt after a failure. {@code null} uses the system default
 * @param headers        arbitrary key-value metadata attached to the task; accessible inside the
 *                       handler via {@link TaskContext#headers()}. Useful for routing, tracing,
 *                       or passing cross-cutting concerns without polluting params
 * @param queue          target queue name; required for unregistered tasks, must match the
 *                       registration queue for registered tasks. {@code null} uses the registration
 *                       or client default queue
 * @param cancellation   auto-cancellation policy defining time limits after which the task is
 *                       automatically cancelled. {@code null} means no automatic cancellation
 * @param idempotencyKey deduplication key; if a non-terminal task with the same key exists in
 *                       the queue, no new task is created and the existing task's IDs are returned.
 *                       {@code null} means no deduplication (every spawn creates a new task)
 */
public record SpawnOptions(
        Integer maxAttempts,
        RetryStrategy retryStrategy,
        Map<String, Object> headers,
        String queue,
        CancellationPolicy cancellation,
        String idempotencyKey
) {

    public static Builder builder() {
        return new Builder();
    }

    public static SpawnOptions defaults() {
        return new Builder().build();
    }

    public static final class Builder {
        private Integer maxAttempts;
        private RetryStrategy retryStrategy;
        private Map<String, Object> headers;
        private String queue;
        private CancellationPolicy cancellation;
        private String idempotencyKey;

        private Builder() {}

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder retryStrategy(RetryStrategy retryStrategy) {
            this.retryStrategy = retryStrategy;
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder cancellation(CancellationPolicy cancellation) {
            this.cancellation = cancellation;
            return this;
        }

        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        public SpawnOptions build() {
            return new SpawnOptions(maxAttempts, retryStrategy, headers, queue, cancellation, idempotencyKey);
        }
    }
}
