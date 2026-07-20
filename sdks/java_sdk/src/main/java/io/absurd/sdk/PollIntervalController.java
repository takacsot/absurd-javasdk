package io.absurd.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.function.Consumer;

/**
 * Controls adaptive poll intervals for the worker loop.
 *
 * <p>When the queue is busy (tasks claimed), the interval snaps back to the minimum.
 * When the queue is empty, the interval linearly increases by a fixed step until
 * the maximum is reached.</p>
 */
final class PollIntervalController {

    private static final Logger log = LoggerFactory.getLogger(PollIntervalController.class);

    private final long minIntervalMs;
    private final long maxIntervalMs;
    private final long stepMs;
    private final Consumer<Duration> onChanged;

    private long currentIntervalMs;

    PollIntervalController(Duration minInterval, Duration maxInterval, Duration step,
                           Consumer<Duration> onChanged) {
        this.minIntervalMs = minInterval.toMillis();
        this.maxIntervalMs = maxInterval.toMillis();
        this.stepMs = step.toMillis();
        this.onChanged = onChanged;
        this.currentIntervalMs = minIntervalMs;
    }

    /**
     * Returns the sleep duration in milliseconds after a poll cycle.
     *
     * @param claimed   number of tasks claimed this cycle
     * @param batchSize the batch size requested
     * @return sleep duration in ms; 0 means skip sleep (full batch)
     */
    long afterPoll(int claimed, int batchSize) {
        if (claimed >= batchSize && batchSize > 0) {
            // Full batch — queue is hot, skip sleep
            setInterval(minIntervalMs);
            return 0;
        }

        if (claimed > 0) {
            // Partial batch — work exists but draining
            setInterval(minIntervalMs);
            return currentIntervalMs;
        }

        // Empty poll — back off linearly
        long next = Math.min(currentIntervalMs + stepMs, maxIntervalMs);
        setInterval(next);
        return currentIntervalMs;
    }

    /**
     * Called on error — treat like empty poll to protect the database.
     */
    long afterError() {
        long next = Math.min(currentIntervalMs + stepMs, maxIntervalMs);
        setInterval(next);
        return currentIntervalMs;
    }

    long currentIntervalMs() {
        return currentIntervalMs;
    }

    private void setInterval(long newIntervalMs) {
        if (newIntervalMs != currentIntervalMs) {
            long oldMs = currentIntervalMs;
            currentIntervalMs = newIntervalMs;
            log.debug("[absurd] Poll interval changed: {}ms → {}ms", oldMs, newIntervalMs);
            if (onChanged != null) {
                onChanged.accept(Duration.ofMillis(newIntervalMs));
            }
        }
    }
}
