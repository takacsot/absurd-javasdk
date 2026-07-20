package io.absurd.sdk;

import java.time.Duration;
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
 *                            Default: 0.25. Lower values reduce latency but increase DB load.
 *                            Ignored when {@code dynamicPolling} is enabled
 * @param onError             callback invoked when a task fails with an unhandled exception;
 *                            receives the exception. Default: no-op. Use for alerting/metrics
 * @param fatalOnLeaseTimeout if {@code true} (default), a lease timeout causes the worker to
 *                            shut down. If {@code false}, the worker logs the timeout and continues.
 *                            Set to {@code false} for resilient long-running workers
 * @param dynamicPolling      enable adaptive polling. When true, the worker backs off linearly
 *                            when idle and snaps back to min interval when work arrives
 * @param minPollInterval     fastest poll rate when queue is busy (default: 25ms)
 * @param maxPollInterval     slowest poll rate when queue is idle (default: 5s)
 * @param pollBackoffStep     amount added to interval after each empty poll (default: 250ms)
 * @param onPollIntervalChanged callback invoked when the effective poll interval changes
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
        boolean pooled,
        boolean dynamicPolling,
        Duration minPollInterval,
        Duration maxPollInterval,
        Duration pollBackoffStep,
        Consumer<Duration> onPollIntervalChanged
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
        private boolean dynamicPolling = false;
        private Duration minPollInterval = Duration.ofMillis(25);
        private Duration maxPollInterval = Duration.ofSeconds(5);
        private Duration pollBackoffStep = Duration.ofMillis(250);
        private Consumer<Duration> onPollIntervalChanged;

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

        public Builder dynamicPolling(boolean dynamicPolling) {
            this.dynamicPolling = dynamicPolling;
            return this;
        }

        public Builder minPollInterval(Duration minPollInterval) {
            this.minPollInterval = minPollInterval;
            return this;
        }

        public Builder maxPollInterval(Duration maxPollInterval) {
            this.maxPollInterval = maxPollInterval;
            return this;
        }

        public Builder pollBackoffStep(Duration pollBackoffStep) {
            this.pollBackoffStep = pollBackoffStep;
            return this;
        }

        public Builder onPollIntervalChanged(Consumer<Duration> onPollIntervalChanged) {
            this.onPollIntervalChanged = onPollIntervalChanged;
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

            if (dynamicPolling) {
                if (minPollInterval.isNegative() || minPollInterval.isZero()) {
                    throw new IllegalArgumentException("minPollInterval must be positive");
                }
                if (maxPollInterval.isNegative() || maxPollInterval.isZero()) {
                    throw new IllegalArgumentException("maxPollInterval must be positive");
                }
                if (minPollInterval.compareTo(maxPollInterval) > 0) {
                    throw new IllegalArgumentException(
                            "minPollInterval (" + minPollInterval.toMillis() + "ms) must be <= maxPollInterval ("
                                    + maxPollInterval.toMillis() + "ms)");
                }
                if (pollBackoffStep.isNegative() || pollBackoffStep.isZero()) {
                    throw new IllegalArgumentException("pollBackoffStep must be positive");
                }
            }

            return new WorkerOptions(effectiveWorkerId, claimTimeout, concurrency, batchSize,
                    pollIntervalSeconds, effectiveOnError, fatalOnLeaseTimeout, shutdownTimeoutSeconds,
                    pooled, dynamicPolling, minPollInterval, maxPollInterval, pollBackoffStep,
                    onPollIntervalChanged);
        }
    }
}
