package io.absurd.sdk;

/**
 * Result of spawning or retrying a task.
 *
 * @param taskID  the task's unique identifier (UUID); stable across retries
 * @param runID   the unique identifier for this specific execution attempt (UUID)
 * @param attempt the attempt number for this run (1-based)
 * @param created {@code true} if a new task was created; {@code false} if an existing task
 *                was returned due to idempotency key deduplication
 */
/**
 * Result of spawning or retrying a task.
 *
 * @param taskID  the task's unique identifier (UUID); stable across retries
 * @param runID   the unique identifier for this specific execution attempt (UUID)
 * @param attempt the attempt number for this run (1-based)
 * @param created {@code true} if a new task was created; {@code false} if an existing task
 *                was returned due to idempotency key deduplication
 */
public record SpawnResult(String taskID, String runID, int attempt, boolean created) {
}
