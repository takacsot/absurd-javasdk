package io.absurd.sdk;

import java.util.function.Consumer;

/**
 * Configuration for a background worker started via {@link Absurd#startWorker}.
 *
 * @param workerId            unique identifier for this worker instance; used for claim tracking
 *                            and debugging. {@code null} auto-generates from hostname:pid
 * @param claimTimeout        seconds a claimed task remains locked before becoming available to
 *                            other workers (visibility timeout). Default: 120. Should exceed your
 *                            longest expected task execution time
 * @param concurrency         maximum number of tasks executed in parallel by this worker.
 *                            Default: 1. Higher values require thread-safe task handlers
 * @param batchSize           number of tasks to claim per poll cycle; {@code null} defaults to
 *                            the concurrency value. Increase for high-throughput scenarios to
 *                            reduce polling overhead
 * @param pollIntervalSeconds seconds between poll attempts when the queue is empty.
 *                            Default: 0.25. Lower values reduce latency but increase DB load
 * @param onError             callback invoked when a task fails with an unhandled exception;
 *                            receives the exception. Default: no-op. Use for alerting/metrics
 * @param fatalOnLeaseTimeout if {@code true} (default), a lease timeout causes the worker to
 *                            shut down. If {@code false}, the worker logs the timeout and continues.
 *                            Set to {@code false} for resilient long-running workers
 */
public record WorkerOptions(
        String workerId,
        int claimTimeout,
        int concurrency,
        Integer batchSize,
        double pollIntervalSeconds,
        Consumer<Exception> onError,
        boolean fatalOnLeaseTimeout,
        int shutdownTimeoutSeconds,
        boolean pooled
) {

    public static Builder builder() {
        return new Builder();
    }

    public static WorkerOptions defaults() {
        return new Builder().build();
    }

    public int effectiveBatchSize() {
        return batchSize != null ? batchSize : concurrency;
    }

    public static final class Builder {
        private String workerId;
        private int claimTimeout = 120;
        private int concurrency = 1;
        private Integer batchSize;
        private double pollIntervalSeconds = 0.25;
        private Consumer<Exception> onError;
        private boolean fatalOnLeaseTimeout = true;
        private int shutdownTimeoutSeconds = 30;
        private boolean pooled = false;

        private Builder() {}

        public Builder workerId(String workerId) {
            this.workerId = workerId;
            return this;
        }

        public Builder claimTimeout(int claimTimeout) {
            this.claimTimeout = claimTimeout;
            return this;
        }

        public Builder concurrency(int concurrency) {
            this.concurrency = concurrency;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder pollIntervalSeconds(double pollIntervalSeconds) {
            this.pollIntervalSeconds = pollIntervalSeconds;
            return this;
        }

        public Builder onError(Consumer<Exception> onError) {
            this.onError = onError;
            return this;
        }

        public Builder fatalOnLeaseTimeout(boolean fatalOnLeaseTimeout) {
            this.fatalOnLeaseTimeout = fatalOnLeaseTimeout;
            return this;
        }

        public Builder shutdownTimeoutSeconds(int shutdownTimeoutSeconds) {
            this.shutdownTimeoutSeconds = shutdownTimeoutSeconds;
            return this;
        }

        public Builder pooled(boolean pooled) {
            this.pooled = pooled;
            return this;
        }

        public WorkerOptions build() {
            String effectiveWorkerId = workerId;
            if (effectiveWorkerId == null) {
                try {
                    effectiveWorkerId = java.net.InetAddress.getLocalHost().getHostName()
                            + ":" + ProcessHandle.current().pid();
                } catch (Exception e) {
                    effectiveWorkerId = "worker:" + ProcessHandle.current().pid();
                }
            }
            Consumer<Exception> effectiveOnError = onError != null ? onError : ex -> {};
            return new WorkerOptions(effectiveWorkerId, claimTimeout, concurrency, batchSize,
                    pollIntervalSeconds, effectiveOnError, fatalOnLeaseTimeout, shutdownTimeoutSeconds, pooled);
        }
    }
}
