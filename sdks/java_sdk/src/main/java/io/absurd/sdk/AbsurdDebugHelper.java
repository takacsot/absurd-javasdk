package io.absurd.sdk;

import org.jdbi.v3.core.Jdbi;

/**
 * Debugging utility for forking a completed/failed task at a specific step checkpoint.
 *
 * <p>Creates a new task with the same params and copies checkpoints up to (and including)
 * the specified step. When executed, the new task skips already-checkpointed steps and
 * resumes fresh from the step after the fork point.</p>
 *
 * <p><b>Warning:</b> This directly manipulates checkpoint tables and is intended for
 * local/staging debugging only. Steps after the fork point will re-execute (including
 * any side effects).</p>
 */
public final class AbsurdDebugHelper {

    private final Absurd absurd;

    public AbsurdDebugHelper(Absurd absurd) {
        this.absurd = absurd;
    }

    /**
     * Forks a task, copying checkpoints up to and including {@code resumeAfterStep}.
     * The returned task can be executed to re-run all steps after the fork point.
     *
     * @param originalTaskId the task ID to fork from
     * @param queue          the queue the original task lives in
     * @param resumeAfterStep the last step to keep; steps after this will re-execute
     * @return the new task's {@link SpawnResult}
     */
    public SpawnResult forkTaskAtStep(String originalTaskId, String queue, String resumeAfterStep) {
        Jdbi jdbi = absurd.jdbi();

        // Get original task info
        var original = jdbi.withHandle(h -> h.createQuery(
                "SELECT task_name, params FROM absurd.t_" + queue + " WHERE task_id = :id::uuid")
            .bind("id", originalTaskId).mapToMap().first());

        String taskName = (String) original.get("task_name");
        Object params = original.get("params");
        JsonValue parsedParams = params == null ? JsonValue.ofNull() : JsonValue.parse(params.toString());

        // Spawn new task
        SpawnResult fork = absurd.spawn(taskName, parsedParams,
            SpawnOptions.builder().maxAttempts(1).queue(queue).build());

        // Copy checkpoints up to the fork point
        jdbi.useHandle(h -> h.createUpdate("""
            INSERT INTO absurd.c_%s (task_id, checkpoint_name, state, status, owner_run_id, updated_at)
            SELECT :newTaskId::uuid, checkpoint_name, state, status, :newRunId::uuid, now()
            FROM absurd.c_%s
            WHERE task_id = :oldTaskId::uuid
              AND updated_at <= (
                SELECT updated_at FROM absurd.c_%s
                WHERE task_id = :oldTaskId::uuid AND checkpoint_name = :step
              )
            """.formatted(queue, queue, queue))
            .bind("newTaskId", fork.taskID())
            .bind("newRunId", fork.runID())
            .bind("oldTaskId", originalTaskId)
            .bind("step", resumeAfterStep)
            .execute());

        return fork;
    }
}
