package io.absurd.sdk;

/**
 * Lifecycle listener for task events. Implement to emit metrics, logging, or tracing.
 *
 * <p>All methods have default no-op implementations so you only override what you need.</p>
 */
public interface TaskLifecycleListener {

    /**
     * Called when a task is registered via {@link Absurd#registerTask}.
     */
    default void onTaskRegistered(String taskName) {}

    /**
     * Called when a task handler begins execution (after claiming).
     */
    default void onTaskStarted(String taskId, String taskName, int attempt) {}

    /**
     * Called when a task handler completes successfully.
     */
    default void onTaskCompleted(String taskId, String taskName, int attempt, long durationMs) {}

    /**
     * Called when a task handler throws an exception.
     */
    default void onTaskFailed(String taskId, String taskName, int attempt, long durationMs, Exception error) {}

    /**
     * Called when a task suspends (sleep or awaitEvent).
     */
    default void onTaskSuspended(String taskId, String taskName, int attempt) {}
}
