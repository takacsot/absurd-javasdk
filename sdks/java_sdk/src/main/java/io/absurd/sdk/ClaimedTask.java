package io.absurd.sdk;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Represents a task claimed by a worker for execution.
 *
 * @param runId         unique ID for this execution attempt (UUID)
 * @param taskId        the parent task's ID (UUID); stable across retries
 * @param taskName      the registered task name determining which handler processes it
 * @param attempt       the current attempt number (1-based)
 * @param params        the task parameters as a JSON tree; deserialized by the framework
 *                      into the handler's parameter type
 * @param retryStrategy the retry backoff configuration for this task (may be null)
 * @param maxAttempts   maximum allowed attempts; {@code null} means unlimited
 * @param headers       arbitrary metadata attached at spawn time; accessible via
 *                      {@link TaskContext#headers()}
 * @param wakeEvent     if the task was sleeping on an event, the event name that woke it;
 *                      {@code null} if woken by timer or freshly spawned
 * @param eventPayload  the payload delivered by the wake event; {@code null} if no event
 */
public record ClaimedTask(
        String runId,
        String taskId,
        String taskName,
        int attempt,
        JsonNode params,
        JsonNode retryStrategy,
        Integer maxAttempts,
        JsonNode headers,
        String wakeEvent,
        JsonNode eventPayload
) {
}
