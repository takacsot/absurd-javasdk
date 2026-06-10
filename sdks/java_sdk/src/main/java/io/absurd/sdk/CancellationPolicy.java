package io.absurd.sdk;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Automatic cancellation policy for tasks that exceed time limits.
 *
 * @param maxDuration maximum total wall-clock seconds from task creation to completion;
 *                    if exceeded, the task is automatically cancelled. {@code null} means
 *                    no duration limit
 * @param maxDelay    maximum seconds a task may remain in pending/sleeping state without
 *                    progressing; if exceeded, the task is cancelled. {@code null} means
 *                    no delay limit. Useful for detecting stuck tasks
 */
/**
 * Automatic cancellation policy for tasks that exceed time limits.
 *
 * @param maxDuration maximum total wall-clock seconds from task creation to completion;
 *                    if exceeded, the task is automatically cancelled. {@code null} means
 *                    no duration limit
 * @param maxDelay    maximum seconds a task may remain in pending/sleeping state without
 *                    progressing; if exceeded, the task is cancelled. {@code null} means
 *                    no delay limit. Useful for detecting stuck tasks
 */
public record CancellationPolicy(Integer maxDuration, Integer maxDelay) {

    public static CancellationPolicy of(Integer maxDuration, Integer maxDelay) {
        return new CancellationPolicy(maxDuration, maxDelay);
    }

    ObjectNode toJson() {
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var node = mapper.createObjectNode();
        if (maxDuration != null) {
            node.put("max_duration", maxDuration);
        }
        if (maxDelay != null) {
            node.put("max_delay", maxDelay);
        }
        return node;
    }
}
