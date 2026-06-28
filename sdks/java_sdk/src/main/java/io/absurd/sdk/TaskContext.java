package io.absurd.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.IntConsumer;

/**
 * Execution context provided to task handlers, enabling durable workflow primitives.
 *
 * <p>TaskContext provides checkpoint-based idempotent steps, sleep/wake coordination,
 * event-driven suspension, and lease management. All state mutations are persisted to
 * PostgreSQL, ensuring durability across process restarts and retries.</p>
 *
 * <p>Supports two connection modes:</p>
 * <ul>
 *   <li><b>Handle-bound</b> (via {@link #create}) — holds a single connection for the lifetime
 *       of task execution. Used by {@link Absurd#workBatch}.</li>
 *   <li><b>Pooled</b> (via {@link #createPooled}) — acquires a connection only for each SQL call
 *       and releases it immediately. No connection is held during user code execution.
 *       Used by {@link Absurd#workBatchPooled}.</li>
 * </ul>
 */
public final class TaskContext implements TaskOperations {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Handle handle;
    private final Jdbi jdbi;
    private final String queueName;
    private final ClaimedTask task;
    private final int claimTimeout;
    private final Map<String, JsonValue> checkpointCache;
    private final Map<String, Integer> stepNameCounter = new HashMap<>();
    private final IntConsumer onLeaseExtended;

    private String mutableWakeEvent;
    private JsonNode mutableEventPayload;

    private TaskContext(Handle handle, Jdbi jdbi, String queueName, ClaimedTask task, int claimTimeout,
                Map<String, JsonValue> checkpointCache, IntConsumer onLeaseExtended) {
        this.handle = handle;
        this.jdbi = jdbi;
        this.queueName = queueName;
        this.task = task;
        this.claimTimeout = claimTimeout;
        this.checkpointCache = checkpointCache;
        this.onLeaseExtended = onLeaseExtended;
        this.mutableWakeEvent = task.wakeEvent();
        this.mutableEventPayload = task.eventPayload();
    }

    /**
     * Creates a handle-bound TaskContext. The connection is held for the task's lifetime.
     */
    static TaskContext create(Handle handle, String queueName, ClaimedTask task,
                              int claimTimeout, IntConsumer onLeaseExtended) {
        Map<String, JsonValue> cache = loadCheckpoints(handle, queueName, task);
        return new TaskContext(handle, null, queueName, task, claimTimeout, cache, onLeaseExtended);
    }

    /**
     * Creates a pooled TaskContext that acquires connections only for individual SQL calls.
     * No connection is held while user task code executes.
     */
    static TaskContext createPooled(Jdbi jdbi, String queueName, ClaimedTask task,
                                    int claimTimeout, IntConsumer onLeaseExtended) {
        Map<String, JsonValue> cache = jdbi.withHandle(h -> loadCheckpoints(h, queueName, task));
        return new TaskContext(null, jdbi, queueName, task, claimTimeout, cache, onLeaseExtended);
    }

    private static Map<String, JsonValue> loadCheckpoints(Handle h, String queueName, ClaimedTask task) {
        var rows = h.createQuery(
                        "SELECT checkpoint_name, state, status, owner_run_id, updated_at " +
                                "FROM absurd.get_task_checkpoint_states(:queue, :taskId::uuid, :runId::uuid)")
                .bind("queue", queueName)
                .bind("taskId", task.taskId())
                .bind("runId", task.runId())
                .mapToMap()
                .list();

        Map<String, JsonValue> cache = new HashMap<>();
        for (var row : rows) {
            String name = (String) row.get("checkpoint_name");
            Object state = row.get("state");
            cache.put(name, state == null ? JsonValue.ofNull() : JsonValue.parse(state.toString()));
        }
        return cache;
    }

    // --- Connection routing ---

    private <T> T withConnection(java.util.function.Function<Handle, T> fn) {
        if (handle != null) {
            return fn.apply(handle);
        }
        return jdbi.withHandle(h -> fn.apply(h));
    }

    private void useConnection(java.util.function.Consumer<Handle> fn) {
        if (handle != null) {
            fn.accept(handle);
            return;
        }
        jdbi.useHandle(h -> fn.accept(h));
    }

    // --- Public API ---

    public String taskID() {
        return task.taskId();
    }

    public Map<String, Object> headers() {
        if (task.headers() == null || task.headers().isNull()) {
            return Collections.emptyMap();
        }
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = MAPPER.treeToValue(task.headers(), Map.class);
            return map != null ? Collections.unmodifiableMap(map) : Collections.emptyMap();
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * Runs an idempotent step; caches and reuses its result across retries.
     */
    public <T> T step(String name, Class<T> resultType, Callable<T> fn) throws Exception {
        StepHandle<T> stepHandle = beginStep(name, resultType);
        if (stepHandle.done()) {
            return stepHandle.state();
        }
        T result = fn.call();
        return completeStep(stepHandle, result);
    }

    /**
     * Step variant that returns JsonValue directly.
     */
    public JsonValue step(String name, Callable<Object> fn) throws Exception {
        String checkpointName = getCheckpointName(name);
        JsonValue cached = lookupCheckpoint(checkpointName);
        if (cached != null) {
            return cached;
        }
        Object result = fn.call();
        JsonValue value = JsonValue.fromObject(result);
        persistCheckpoint(checkpointName, value);
        return value;
    }

    public <T> StepHandle<T> beginStep(String name, Class<T> resultType) {
        String checkpointName = getCheckpointName(name);
        JsonValue cached = lookupCheckpoint(checkpointName);
        if (cached != null) {
            T state = cached.as(resultType);
            return StepHandle.completed(name, checkpointName, state);
        }
        return StepHandle.pending(name, checkpointName);
    }

    public <T> T completeStep(StepHandle<T> stepHandle, T value) {
        if (stepHandle.done()) {
            return stepHandle.state();
        }
        persistCheckpoint(stepHandle.checkpointName(), JsonValue.fromObject(value));
        return value;
    }

    /**
     * Suspends the task until the given duration elapses.
     */
    public void sleepFor(String stepName, Duration duration) {
        sleepUntil(stepName, Instant.now().plus(duration));
    }

    /**
     * Suspends the task until the given duration in seconds elapses.
     */
    public void sleepFor(String stepName, int seconds) {
        sleepFor(stepName, Duration.ofSeconds(seconds));
    }

    /**
     * Suspends the task until the specified time.
     */
    public void sleepUntil(String stepName, Instant wakeAt) {
        String checkpointName = getCheckpointName(stepName);
        JsonValue cached = lookupCheckpoint(checkpointName);
        Instant actualWakeAt;
        if (cached != null && !cached.isNull()) {
            actualWakeAt = Instant.parse(cached.node().asText());
        } else {
            actualWakeAt = wakeAt;
            persistCheckpoint(checkpointName, JsonValue.fromObject(wakeAt.toString()));
        }

        double remainingSeconds = Duration.between(Instant.now(), actualWakeAt).toMillis() / 1000.0;
        if (remainingSeconds > 0) {
            scheduleRunAfter(remainingSeconds);
            throw new SuspendTaskException();
        }
    }

    /**
     * Waits for an event by name and returns its payload.
     */
    public JsonValue awaitEvent(String eventName) {
        return awaitEvent(eventName, null, null);
    }

    /**
     * Waits for an event by name with a timeout.
     */
    public JsonValue awaitEvent(String eventName, Integer timeoutSeconds) {
        return awaitEvent(eventName, null, timeoutSeconds);
    }

    /**
     * Waits for an event by name with a custom step name and optional timeout.
     */
    public JsonValue awaitEvent(String eventName, String stepName, Integer timeoutSeconds) {
        String effectiveStepName = stepName != null ? stepName : "$awaitEvent:" + eventName;
        Integer timeout = null;
        if (timeoutSeconds != null && timeoutSeconds >= 0) {
            timeout = timeoutSeconds;
        }

        String checkpointName = getCheckpointName(effectiveStepName);
        JsonValue cached = lookupCheckpoint(checkpointName);
        if (cached != null) {
            return cached;
        }

        if (eventName.equals(mutableWakeEvent) &&
                (mutableEventPayload == null || mutableEventPayload.isNull())) {
            mutableWakeEvent = null;
            mutableEventPayload = null;
            throw new TimeoutException("Timed out waiting for event \"" + eventName + "\"");
        }

        var result = queryWithTaskStateCheck(
                "SELECT should_suspend, payload FROM absurd.await_event(:queue, :taskId::uuid, :runId::uuid, :checkpoint, :event, :timeout)",
                Map.of(
                        "queue", queueName,
                        "taskId", task.taskId(),
                        "runId", task.runId(),
                        "checkpoint", checkpointName,
                        "event", eventName
                ),
                timeout
        );

        if (result == null) {
            throw new AbsurdException("Failed to await event");
        }

        boolean shouldSuspend = (Boolean) result.get("should_suspend");
        if (!shouldSuspend) {
            Object payload = result.get("payload");
            JsonValue payloadValue = payload == null ? JsonValue.ofNull() : JsonValue.parse(payload.toString());
            checkpointCache.put(checkpointName, payloadValue);
            mutableEventPayload = null;
            return payloadValue;
        }

        throw new SuspendTaskException();
    }

    /**
     * Extends the current run's lease.
     */
    public void heartbeat() {
        heartbeat(claimTimeout);
    }

    /**
     * Extends the current run's lease by the given seconds.
     */
    public void heartbeat(int seconds) {
        try {
            useConnection(h ->
                h.createUpdate("SELECT absurd.extend_claim(:queue, :runId::uuid, :seconds)")
                    .bind("queue", queueName)
                    .bind("runId", task.runId())
                    .bind("seconds", seconds)
                    .execute()
            );
        } catch (Exception e) {
            throw mapTaskStateError(e);
        }
        onLeaseExtended.accept(seconds);
    }

    /**
     * Emits an event with an optional payload.
     */
    public void emitEvent(String eventName, Object payload) {
        if (eventName == null || eventName.isEmpty()) {
            throw new AbsurdException("eventName must be a non-empty string");
        }
        String payloadJson = JsonValue.fromObject(payload).toJson();
        useConnection(h ->
            h.createUpdate("SELECT absurd.emit_event(:queue, :event, :payload::jsonb)")
                .bind("queue", queueName)
                .bind("event", eventName)
                .bind("payload", payloadJson)
                .execute()
        );
    }

    public void emitEvent(String eventName) {
        emitEvent(eventName, null);
    }

    /**
     * Spawns a child task and polls until it reaches a terminal state, returning the snapshot.
     *
     * <p>The child must be on a different queue than this task's queue to avoid deadlocking
     * workers. Automatically sends heartbeats to keep the parent lease alive while waiting.</p>
     *
     * @param taskID         the child task's ID (already spawned)
     * @param queue          the queue the child lives in; must differ from this task's queue
     * @param timeoutSeconds max seconds to wait; {@code null} means wait indefinitely
     * @return the terminal {@link TaskResultSnapshot}
     * @throws AbsurdException  if queue matches this task's queue (deadlock guard)
     * @throws TimeoutException if timeout elapses before the child completes
     */
    public TaskResultSnapshot awaitTaskResult(String taskID, String queue, Integer timeoutSeconds) {
        if (queue == null || queue.equals(queueName)) {
            throw new AbsurdException(
                "TaskContext.awaitTaskResult cannot wait on tasks in the same queue because this can deadlock workers. " +
                "Spawn the child in a different queue and pass the child's queue.");
        }

        String stepName = "$awaitTaskResult:" + taskID;
        String checkpointName = getCheckpointName(stepName);
        JsonValue cached = lookupCheckpoint(checkpointName);
        if (cached != null) {
            return snapshotFromJson(cached);
        }

        // Poll until terminal
        long heartbeatIntervalMs = Math.max(500, (claimTimeout * 1000L) / 2);
        long nextHeartbeatAt = System.currentTimeMillis() + heartbeatIntervalMs;
        Long timeoutMs = (timeoutSeconds != null && timeoutSeconds >= 0)
                ? (long) timeoutSeconds * 1000 : null;
        long startedAt = System.currentTimeMillis();
        long delayMs = 50;

        while (true) {
            TaskResultSnapshot snapshot = withConnection(h -> {
                var rows = h.createQuery(
                        "SELECT state, result, failure_reason FROM absurd.get_task_result(:queue, :taskId::uuid)")
                    .bind("queue", queue)
                    .bind("taskId", taskID)
                    .mapToMap()
                    .list();
                if (rows.isEmpty()) return null;
                var row = rows.get(0);
                String state = (String) row.get("state");
                return switch (state) {
                    case "completed" -> {
                        Object result = row.get("result");
                        yield new TaskResultSnapshot.Completed(
                            result == null ? JsonValue.ofNull() : JsonValue.parse(result.toString()));
                    }
                    case "failed" -> {
                        Object failure = row.get("failure_reason");
                        yield new TaskResultSnapshot.Failed(
                            failure == null ? JsonValue.ofNull() : JsonValue.parse(failure.toString()));
                    }
                    case "cancelled" -> new TaskResultSnapshot.Cancelled();
                    case "running" -> new TaskResultSnapshot.Running();
                    case "sleeping" -> new TaskResultSnapshot.Sleeping();
                    default -> new TaskResultSnapshot.Pending();
                };
            });

            if (snapshot == null) {
                throw new AbsurdException("Task \"" + taskID + "\" not found");
            }
            if (TaskResultSnapshot.isTerminal(snapshot)) {
                // Persist as checkpoint
                JsonValue value = snapshotToJson(snapshot);
                persistCheckpoint(checkpointName, value);
                return snapshot;
            }

            // Heartbeat to keep parent alive
            long now = System.currentTimeMillis();
            if (now >= nextHeartbeatAt) {
                heartbeat();
                nextHeartbeatAt = System.currentTimeMillis() + heartbeatIntervalMs;
            }

            if (timeoutMs != null) {
                long elapsed = System.currentTimeMillis() - startedAt;
                if (elapsed >= timeoutMs) {
                    throw new TimeoutException("Timed out waiting for task \"" + taskID + "\"");
                }
                delayMs = Math.min(delayMs, timeoutMs - elapsed);
            }

            try {
                Thread.sleep(Math.max(0, delayMs));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AbsurdException("Interrupted while waiting for child task", e);
            }
            delayMs = Math.min(delayMs * 2, 1000);
        }
    }

    // --- Internals ---

    private String getCheckpointName(String name) {
        int count = stepNameCounter.merge(name, 1, Integer::sum);
        return count == 1 ? name : name + "#" + count;
    }

    private JsonValue lookupCheckpoint(String checkpointName) {
        JsonValue cached = checkpointCache.get(checkpointName);
        if (cached != null) {
            return cached;
        }

        var rows = withConnection(h ->
            h.createQuery(
                    "SELECT checkpoint_name, state, status, owner_run_id, updated_at " +
                            "FROM absurd.get_task_checkpoint_state(:queue, :taskId::uuid, :checkpoint)")
                .bind("queue", queueName)
                .bind("taskId", task.taskId())
                .bind("checkpoint", checkpointName)
                .mapToMap()
                .list()
        );

        if (!rows.isEmpty()) {
            Object state = rows.get(0).get("state");
            JsonValue value = state == null ? JsonValue.ofNull() : JsonValue.parse(state.toString());
            checkpointCache.put(checkpointName, value);
            return value;
        }
        return null;
    }

    private void persistCheckpoint(String checkpointName, JsonValue value) {
        try {
            useConnection(h ->
                h.createUpdate(
                        "SELECT absurd.set_task_checkpoint_state(:queue, :taskId::uuid, :checkpoint, :state::jsonb, :runId::uuid, :claimTimeout)")
                    .bind("queue", queueName)
                    .bind("taskId", task.taskId())
                    .bind("checkpoint", checkpointName)
                    .bind("state", value.toJson())
                    .bind("runId", task.runId())
                    .bind("claimTimeout", claimTimeout)
                    .execute()
            );
        } catch (Exception e) {
            throw mapTaskStateError(e);
        }
        checkpointCache.put(checkpointName, value);
        onLeaseExtended.accept(claimTimeout);
    }

    private void scheduleRunAfter(double seconds) {
        useConnection(h ->
            h.createUpdate("SELECT absurd.schedule_run(:queue, :runId::uuid, absurd.current_time() + make_interval(secs => :seconds::double precision))")
                .bind("queue", queueName)
                .bind("runId", task.runId())
                .bind("seconds", seconds)
                .execute()
        );
    }

    private Map<String, Object> queryWithTaskStateCheck(String sql, Map<String, Object> bindings, Integer timeout) {
        try {
            return withConnection(h -> {
                var query = h.createQuery(sql);
                bindings.forEach(query::bind);
                if (timeout != null) {
                    query.bind("timeout", timeout);
                } else {
                    query.bindNull("timeout", java.sql.Types.INTEGER);
                }
                var rows = query.mapToMap().list();
                return rows.isEmpty() ? null : rows.get(0);
            });
        } catch (Exception e) {
            throw mapTaskStateError(e);
        }
    }

    private static RuntimeException mapTaskStateError(Exception e) {
        String message = e.getMessage();
        if (message != null) {
            if (message.contains("AB001")) {
                return new CancelledTaskException();
            }
            if (message.contains("AB002")) {
                return new FailedTaskException();
            }
        }
        if (e instanceof RuntimeException re) {
            return re;
        }
        return new AbsurdException("Database error: " + message, e);
    }

    private static JsonValue snapshotToJson(TaskResultSnapshot snapshot) {
        if (snapshot instanceof TaskResultSnapshot.Completed c) {
            return JsonValue.fromObject(Map.of("state", "completed", "result", c.result().node()));
        } else if (snapshot instanceof TaskResultSnapshot.Failed f) {
            return JsonValue.fromObject(Map.of("state", "failed", "failure", f.failure().node()));
        } else if (snapshot instanceof TaskResultSnapshot.Cancelled) {
            return JsonValue.fromObject(Map.of("state", "cancelled"));
        } else {
            return JsonValue.fromObject(Map.of("state", snapshot.state()));
        }
    }

    private static TaskResultSnapshot snapshotFromJson(JsonValue value) {
        var node = value.node();
        String state = node.get("state").asText();
        if ("completed".equals(state)) {
            return new TaskResultSnapshot.Completed(
                    node.has("result") ? JsonValue.of(node.get("result")) : JsonValue.ofNull());
        } else if ("failed".equals(state)) {
            return new TaskResultSnapshot.Failed(
                    node.has("failure") ? JsonValue.of(node.get("failure")) : JsonValue.ofNull());
        } else if ("cancelled".equals(state)) {
            return new TaskResultSnapshot.Cancelled();
        } else {
            return new TaskResultSnapshot.Pending();
        }
    }
}
