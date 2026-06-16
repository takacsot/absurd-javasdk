package io.absurd.sdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.Handle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Absurd SDK client for durable task processing backed by PostgreSQL.
 *
 * <p>Absurd provides durable, retryable task queues with checkpoint-based resumption,
 * event-driven coordination, and automatic retry strategies. Tasks survive process restarts
 * and can be distributed across multiple workers.</p>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * var absurd = Absurd.create(dataSource, "my-queue");
 * absurd.registerTask("send_email", EmailParams.class, (params, ctx) -> {
 *     sendEmail(params);
 *     return null;
 * });
 * absurd.startWorker();
 * }</pre>
 *
 * @see TaskHandler
 * @see TaskContext
 * @see SpawnOptions
 */
@Slf4j
public final class Absurd implements AutoCloseable {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_QUEUE_NAME_LENGTH = 57;
    private static final int UNKNOWN_TASK_DEFER_BASE_SECONDS = 15;
    private static final int UNKNOWN_TASK_DEFER_JITTER_SECONDS = 15;

    private final Jdbi jdbi;
    private final String queueName;
    private final int defaultMaxAttempts;
    private final List<TaskLifecycleListener> listeners;
    private final ConcurrentHashMap<String, TaskRegistration> registry = new ConcurrentHashMap<>();
    private volatile Worker worker;

    private Absurd(Jdbi jdbi, String queueName, int defaultMaxAttempts, List<TaskLifecycleListener> listeners) {
        this.jdbi = jdbi;
        this.queueName = validateQueueName(queueName);
        this.defaultMaxAttempts = defaultMaxAttempts;
        this.listeners = listeners != null ? List.copyOf(listeners) : List.of();
    }

    public static Absurd create(DataSource dataSource) {
        return create(dataSource, "default");
    }

    public static Absurd create(DataSource dataSource, String queueName) {
        return create(dataSource, queueName, 5);
    }

    /**
     * Creates an Absurd client with the given DataSource, queue name, and default max attempts.
     *
     * @param dataSource       the JDBC DataSource for PostgreSQL connections
     * @param queueName        the default queue name for task operations (max 57 UTF-8 bytes);
     *                         tasks without an explicit queue will use this
     * @param defaultMaxAttempts the default maximum number of execution attempts before a task
     *                         is permanently failed (used when neither spawn options nor task
     *                         registration specify a value)
     * @return a new Absurd client instance
     * @throws AbsurdException if the queue name is null, empty, or exceeds 57 bytes
     */
    public static Absurd create(DataSource dataSource, String queueName, int defaultMaxAttempts) {
        Jdbi jdbi = Jdbi.create(dataSource);
        return new Absurd(jdbi, queueName, defaultMaxAttempts, null);
    }

    public static Absurd create(Jdbi jdbi) {
        return create(jdbi, "default");
    }

    public static Absurd create(Jdbi jdbi, String queueName) {
        return create(jdbi, queueName, 5);
    }

    public static Absurd create(Jdbi jdbi, String queueName, int defaultMaxAttempts) {
        return new Absurd(jdbi, queueName, defaultMaxAttempts, null);
    }

    public static AbsurdBuilder builder(DataSource dataSource) {
        return new AbsurdBuilder(Jdbi.create(dataSource));
    }

    public static AbsurdBuilder builder(Jdbi jdbi) {
        return new AbsurdBuilder(jdbi);
    }

    public static final class AbsurdBuilder {
        private final Jdbi jdbi;
        private String queueName = "default";
        private int defaultMaxAttempts = 5;
        private final List<TaskLifecycleListener> listeners = new ArrayList<>();

        private AbsurdBuilder(Jdbi jdbi) {
            this.jdbi = jdbi;
        }

        public AbsurdBuilder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public AbsurdBuilder defaultMaxAttempts(int defaultMaxAttempts) {
            this.defaultMaxAttempts = defaultMaxAttempts;
            return this;
        }

        public AbsurdBuilder listener(TaskLifecycleListener listener) {
            this.listeners.add(listener);
            return this;
        }

        public Absurd build() {
            return new Absurd(jdbi, queueName, defaultMaxAttempts, listeners);
        }
    }

    public Jdbi jdbi() {
        return jdbi;
    }

    public String queueName() {
        return queueName;
    }

    // --- Task Registration ---

    public void registerTask(TaskRegistration registration) {
        String effectiveQueue = registration.queue() != null ? registration.queue() : queueName;
        var effective = new TaskRegistration(
                registration.name(),
                validateQueueName(effectiveQueue),
                registration.defaultMaxAttempts(),
                registration.defaultCancellation(),
                registration.paramsType(),
                registration.handler()
        );
        registry.put(registration.name(), effective);
        notifyListeners(l -> l.onTaskRegistered(registration.name()));
    }

    public <P, R> void registerTask(String name, Class<P> paramsType, TaskHandler<P, R> handler) {
        registerTask(TaskRegistration.builder(name).handler(paramsType, handler).build());
    }

    // --- Queue Management ---

    public void createQueue() {
        createQueue(queueName);
    }

    public void createQueue(String queueName) {
        createQueue(queueName, "unpartitioned");
    }

    /**
     * Creates a new task queue with the specified storage mode.
     *
     * @param queueName   the queue name (max 57 UTF-8 bytes); used as a prefix for
     *                    underlying database tables (t_, r_, c_, e_, w_)
     * @param storageMode the PostgreSQL table partitioning strategy:
     *                    {@code "unpartitioned"} for single table per queue (default, simpler)
     * @throws AbsurdException if the queue name is invalid
     */
    public void createQueue(String queueName, String storageMode) {
        String queue = validateQueueName(queueName);
        jdbi.useHandle(h -> {
            if ("unpartitioned".equals(storageMode)) {
                h.createUpdate("SELECT absurd.create_queue(:queue)")
                        .bind("queue", queue)
                        .execute();
            } else {
                h.createUpdate("SELECT absurd.create_queue(:queue, :mode)")
                        .bind("queue", queue)
                        .bind("mode", storageMode)
                        .execute();
            }
        });
    }

    public void dropQueue(String queueName) {
        String queue = validateQueueName(queueName);
        jdbi.useHandle(h ->
                h.createUpdate("SELECT absurd.drop_queue(:queue)")
                        .bind("queue", queue)
                        .execute()
        );
    }

    public List<String> listQueues() {
        return jdbi.withHandle(h ->
                h.createQuery("SELECT * FROM absurd.list_queues()")
                        .map((rs, ctx) -> rs.getString("queue_name"))
                        .list()
        );
    }

    // --- Spawning ---

    public SpawnResult spawn(String taskName, Object params) {
        return spawn(taskName, params, SpawnOptions.defaults());
    }

    /**
     * Spawns a new task for asynchronous execution.
     *
     * <p>The task is persisted to the database and will be picked up by a worker. If the task
     * name is registered, its queue and retry settings are used as defaults. Unregistered tasks
     * require {@link SpawnOptions#queue()} to be set.</p>
     *
     * @param taskName the registered task name identifying which handler processes this task
     * @param params   the task parameters, serialized to JSON; can be any Jackson-serializable
     *                 object, a {@link JsonValue}, or {@code null}
     * @param options  spawn configuration overriding registration defaults
     * @return a {@link SpawnResult} containing the task ID, run ID, attempt number, and whether
     *         a new task was created (false if deduplicated by idempotency key)
     * @throws AbsurdException if the task is unregistered and no queue is specified, or if the
     *                         requested queue conflicts with the registration
     */
    public SpawnResult spawn(String taskName, Object params, SpawnOptions options) {
        var registration = registry.get(taskName);
        String queue;

        if (registration != null) {
            queue = registration.queue() != null ? registration.queue() : queueName;
            if (options.queue() != null) {
                String requestedQueue = validateQueueName(options.queue());
                if (!requestedQueue.equals(queue)) {
                    throw new AbsurdException(
                            "Task \"" + taskName + "\" is registered for queue \"" + queue +
                                    "\" but spawn requested queue \"" + options.queue() + "\".");
                }
            }
        } else if (options.queue() == null) {
            throw new AbsurdException(
                    "Task \"" + taskName + "\" is not registered. Provide options.queue when spawning unregistered tasks.");
        } else {
            queue = validateQueueName(options.queue());
        }

        int effectiveMaxAttempts = options.maxAttempts() != null
                ? options.maxAttempts()
                : (registration != null && registration.defaultMaxAttempts() != null
                ? registration.defaultMaxAttempts()
                : defaultMaxAttempts);

        CancellationPolicy effectiveCancellation = options.cancellation() != null
                ? options.cancellation()
                : (registration != null ? registration.defaultCancellation() : null);

        ObjectNode normalizedOptions = buildSpawnPayload(options, effectiveMaxAttempts, effectiveCancellation);
        String paramsJson = JsonValue.fromObject(params).toJson();
        String optionsJson = normalizedOptions.toString();

        return jdbi.withHandle(h -> {
            var row = h.createQuery(
                            "SELECT task_id, run_id, attempt, created FROM absurd.spawn_task(:queue, :taskName, :params::jsonb, :options::jsonb)")
                    .bind("queue", queue)
                    .bind("taskName", taskName)
                    .bind("params", paramsJson)
                    .bind("options", optionsJson)
                    .mapToMap()
                    .first();

            return new SpawnResult(
                    row.get("task_id").toString(),
                    row.get("run_id").toString(),
                    ((Number) row.get("attempt")).intValue(),
                    (Boolean) row.get("created")
            );
        });
    }

    /**
     * Spawns a task using an externally-managed database connection/transaction.
     *
     * <p>This enables the transactional outbox pattern: the task enqueue participates in the
     * caller's transaction. If the transaction commits, the task is guaranteed to be persisted.
     * If it rolls back, the task spawn is also rolled back.</p>
     *
     * @param handle   the caller's JDBI Handle, typically inside an active transaction
     * @param taskName the registered task name identifying which handler processes this task
     * @param params   the task parameters, serialized to JSON
     * @return a {@link SpawnResult}
     */
    public SpawnResult spawn(Handle handle, String taskName, Object params) {
        return spawn(handle, taskName, params, SpawnOptions.defaults());
    }

    /**
     * Spawns a task using an externally-managed connection with full spawn options.
     *
     * @param handle   the caller's JDBI Handle, typically inside an active transaction
     * @param taskName the registered task name
     * @param params   the task parameters, serialized to JSON
     * @param options  spawn configuration (maxAttempts, retryStrategy, headers, etc.)
     * @return a {@link SpawnResult}
     */
    public SpawnResult spawn(Handle handle, String taskName, Object params, SpawnOptions options) {
        var registration = registry.get(taskName);
        String queue;

        if (registration != null) {
            queue = registration.queue() != null ? registration.queue() : queueName;
            if (options.queue() != null) {
                String requestedQueue = validateQueueName(options.queue());
                if (!requestedQueue.equals(queue)) {
                    throw new AbsurdException(
                            "Task \"" + taskName + "\" is registered for queue \"" + queue +
                                    "\" but spawn requested queue \"" + options.queue() + "\".");
                }
            }
        } else if (options.queue() == null) {
            throw new AbsurdException(
                    "Task \"" + taskName + "\" is not registered. Provide options.queue when spawning unregistered tasks.");
        } else {
            queue = validateQueueName(options.queue());
        }

        int effectiveMaxAttempts = options.maxAttempts() != null
                ? options.maxAttempts()
                : (registration != null && registration.defaultMaxAttempts() != null
                ? registration.defaultMaxAttempts()
                : defaultMaxAttempts);

        CancellationPolicy effectiveCancellation = options.cancellation() != null
                ? options.cancellation()
                : (registration != null ? registration.defaultCancellation() : null);

        ObjectNode normalizedOptions = buildSpawnPayload(options, effectiveMaxAttempts, effectiveCancellation);
        String paramsJson = JsonValue.fromObject(params).toJson();
        String optionsJson = normalizedOptions.toString();

        var row = handle.createQuery(
                        "SELECT task_id, run_id, attempt, created FROM absurd.spawn_task(:queue, :taskName, :params::jsonb, :options::jsonb)")
                .bind("queue", queue)
                .bind("taskName", taskName)
                .bind("params", paramsJson)
                .bind("options", optionsJson)
                .mapToMap()
                .first();

        return new SpawnResult(
                row.get("task_id").toString(),
                row.get("run_id").toString(),
                ((Number) row.get("attempt")).intValue(),
                (Boolean) row.get("created")
        );
    }

    /**
     * Spawns a task using a plain JDBC Connection (transactional outbox pattern).
     *
     * <p>Wraps the connection in a JDBI Handle and delegates to {@link #spawn(Handle, String, Object)}.
     * The connection is NOT closed by this method — the caller retains ownership.</p>
     *
     * @param connection the caller's JDBC Connection, typically inside an active transaction
     * @param taskName   the registered task name
     * @param params     the task parameters, serialized to JSON
     * @return a {@link SpawnResult}
     */
    public SpawnResult spawn(Connection connection, String taskName, Object params) {
        return spawn(connection, taskName, params, SpawnOptions.defaults());
    }

    /**
     * Spawns a task using a plain JDBC Connection with full spawn options.
     *
     * @param connection the caller's JDBC Connection, typically inside an active transaction
     * @param taskName   the registered task name
     * @param params     the task parameters, serialized to JSON
     * @param options    spawn configuration
     * @return a {@link SpawnResult}
     */
    public SpawnResult spawn(Connection connection, String taskName, Object params, SpawnOptions options) {
        Handle handle = Jdbi.open(connection);
        return spawn(handle, taskName, params, options);
    }

    // --- Events ---

    public void emitEvent(String eventName) {
        emitEvent(eventName, null, null);
    }

    public void emitEvent(String eventName, Object payload) {
        emitEvent(eventName, payload, null);
    }

    /**
     * Emits a named event that can wake tasks suspended via {@link TaskContext#awaitEvent}.
     *
     * @param eventName the event name; must be non-empty. Tasks waiting on this exact name
     *                  in the target queue will be resumed
     * @param payload   optional event payload delivered to the waiting task; any
     *                  Jackson-serializable object or {@code null}
     * @param queueName the queue to emit the event into; if {@code null}, uses the client's
     *                  default queue
     * @throws AbsurdException if eventName is null or empty
     */
    public void emitEvent(String eventName, Object payload, String queueName) {
        if (eventName == null || eventName.isEmpty()) {
            throw new AbsurdException("eventName must be a non-empty string");
        }
        String queue = validateQueueName(queueName != null ? queueName : this.queueName);
        String payloadJson = JsonValue.fromObject(payload).toJson();
        jdbi.useHandle(h ->
                h.createUpdate("SELECT absurd.emit_event(:queue, :event, :payload::jsonb)")
                        .bind("queue", queue)
                        .bind("event", eventName)
                        .bind("payload", payloadJson)
                        .execute()
        );
    }

    // --- Task Results ---

    public TaskResultSnapshot fetchTaskResult(String taskID) {
        return fetchTaskResult(taskID, queueName);
    }

    public TaskResultSnapshot fetchTaskResult(String taskID, String queueName) {
        String queue = validateQueueName(queueName);
        return jdbi.withHandle(h -> fetchTaskResultSnapshot(h, queue, taskID));
    }

    public TaskResultSnapshot awaitTaskResult(String taskID) {
        return awaitTaskResult(taskID, queueName, null);
    }

    /**
     * Blocks until the task reaches a terminal state (completed, failed, or cancelled).
     *
     * <p>Uses exponential backoff polling starting at 50ms, capped at 1s.</p>
     *
     * @param taskID         the UUID of the task to wait for
     * @param queueName      the queue containing the task; if {@code null}, uses client default
     * @param timeoutSeconds maximum seconds to wait; if {@code null}, waits indefinitely
     * @return a {@link TaskResultSnapshot} representing the terminal state
     * @throws TimeoutException if the timeout elapses before the task completes
     * @throws AbsurdException  if the task is not found
     */
    public TaskResultSnapshot awaitTaskResult(String taskID, String queueName, Integer timeoutSeconds) {
        String queue = validateQueueName(queueName != null ? queueName : this.queueName);
        return awaitTaskResultWithBackoff(queue, taskID, timeoutSeconds);
    }

    // --- Retry / Cancel ---

    public SpawnResult retryTask(String taskID) {
        return retryTask(taskID, queueName, null, false);
    }

    /**
     * Retries a failed or cancelled task, creating a new run.
     *
     * @param taskID      the UUID of the task to retry
     * @param queueName   the queue containing the task; if {@code null}, uses the client's default
     * @param maxAttempts optional new max attempts limit; if {@code null}, preserves the original
     * @param spawnNew    if {@code true}, creates a brand-new task (new task ID) rather than
     *                    adding a run to the existing task
     * @return a {@link SpawnResult} with the new run details
     */
    public SpawnResult retryTask(String taskID, String queueName, Integer maxAttempts, boolean spawnNew) {
        String queue = validateQueueName(queueName != null ? queueName : this.queueName);
        ObjectNode payload = MAPPER.createObjectNode();
        if (maxAttempts != null) {
            payload.put("max_attempts", maxAttempts);
        }
        if (spawnNew) {
            payload.put("spawn_new", true);
        }

        return jdbi.withHandle(h -> {
            var row = h.createQuery(
                            "SELECT task_id, run_id, attempt, created FROM absurd.retry_task(:queue, :taskId::uuid, :payload::jsonb)")
                    .bind("queue", queue)
                    .bind("taskId", taskID)
                    .bind("payload", payload.toString())
                    .mapToMap()
                    .first();

            return new SpawnResult(
                    row.get("task_id").toString(),
                    row.get("run_id").toString(),
                    ((Number) row.get("attempt")).intValue(),
                    (Boolean) row.get("created")
            );
        });
    }

    public void cancelTask(String taskID) {
        cancelTask(taskID, null);
    }

    public void cancelTask(String taskID, String queueName) {
        String queue = validateQueueName(queueName != null ? queueName : this.queueName);
        jdbi.useHandle(h ->
                h.createUpdate("SELECT absurd.cancel_task(:queue, :taskId::uuid)")
                        .bind("queue", queue)
                        .bind("taskId", taskID)
                        .execute()
        );
    }

    // --- Claiming ---

    public List<ClaimedTask> claimTasks(int batchSize, int claimTimeout, String workerId) {
        return jdbi.withHandle(h -> claimTasksWithHandle(h, batchSize, claimTimeout, workerId));
    }

    List<ClaimedTask> claimTasksWithHandle(Handle h, int batchSize, int claimTimeout, String workerId) {
        var rows = h.createQuery(
                        "SELECT run_id, task_id, attempt, task_name, params, retry_strategy, max_attempts, " +
                                "headers, wake_event, event_payload " +
                                "FROM absurd.claim_task(:queue, :workerId, :claimTimeout, :count)")
                .bind("queue", queueName)
                .bind("workerId", workerId)
                .bind("claimTimeout", claimTimeout)
                .bind("count", batchSize)
                .mapToMap()
                .list();

        List<ClaimedTask> tasks = new ArrayList<>();
        for (var row : rows) {
            tasks.add(mapClaimedTask(row));
        }
        return tasks;
    }

    // --- Work Batch ---

    /**
     * Claims and executes a batch of tasks in a single database transaction.
     *
     * <p>Claimed tasks are locked for {@code claimTimeout} seconds. If execution exceeds
     * the timeout without a heartbeat, the task becomes available for other workers.</p>
     *
     * @param workerId     unique identifier for this worker instance (visible as "claimed_by")
     * @param claimTimeout seconds the task remains claimed before becoming available again
     *                     if no heartbeat is sent; acts as a visibility timeout
     * @param batchSize    maximum number of tasks to claim and execute in this batch
     */
    public void workBatch(String workerId, int claimTimeout, int batchSize) {
        jdbi.useHandle(h -> {
            var tasks = claimTasksWithHandle(h, batchSize, claimTimeout, workerId);
            for (var task : tasks) {
                executeTask(h, task, claimTimeout);
            }
        });
    }

    /**
     * Claims and executes a batch of tasks using pool-per-query semantics.
     *
     * <p>Connections are only held for the duration of individual SQL calls (claim, checkpoint,
     * complete, fail). No connection is held while the task handler executes user code.
     * This matches the TypeScript SDK's connection pattern and is better suited for
     * long-running tasks or high-concurrency workers.</p>
     *
     * @param workerId     unique identifier for this worker instance
     * @param claimTimeout seconds the task remains claimed (visibility timeout)
     * @param batchSize    maximum number of tasks to claim and execute
     */
    public void workBatchPooled(String workerId, int claimTimeout, int batchSize) {
        List<ClaimedTask> tasks = jdbi.withHandle(h ->
            claimTasksWithHandle(h, batchSize, claimTimeout, workerId));
        for (ClaimedTask task : tasks) {
            executeTaskPooled(task, claimTimeout);
        }
    }

    void executeTaskPooled(ClaimedTask task, int claimTimeout) {
        executeTaskPooled(task, claimTimeout, leaseSeconds -> {});
    }

    void executeTaskPooled(ClaimedTask task, int claimTimeout, java.util.function.IntConsumer onLeaseExtended) {
        var registration = registry.get(task.taskName());
        String taskLabel = task.taskName() + " (" + task.taskId() + ")";

        try {
            if (registration == null) {
                try {
                    int deferSeconds = jdbi.withHandle(h -> deferClaimedRun(h, task.runId(), task.runId()));
                    log.warn("Claimed unknown task \"{}\"; deferred run {} by {}s",
                            task.taskName(), task.runId(), deferSeconds);
                    return;
                } catch (Exception deferErr) {
                    log.error("Failed to defer unknown task \"{}\"; failing run", task.taskName(), deferErr);
                    try {
                        jdbi.useHandle(h -> failTaskRun(h, task.runId(), deferErr));
                    } catch (CancelledTaskException | FailedTaskException ignored) {}
                    return;
                }
            }

            String effectiveQueue = registration.queue() != null ? registration.queue() : queueName;
            if (!effectiveQueue.equals(queueName)) {
                throw new AbsurdException("Misconfigured task (queue mismatch)");
            }

            TaskContext ctx = TaskContext.createPooled(jdbi, queueName, task, claimTimeout, onLeaseExtended);

            Object params;
            if (registration.paramsType() == JsonValue.class) {
                params = task.params() != null ? JsonValue.of(task.params()) : JsonValue.ofNull();
            } else {
                params = MAPPER.treeToValue(
                        task.params() != null ? task.params() : NullNode.getInstance(),
                        registration.paramsType()
                );
            }

            notifyListeners(l -> l.onTaskStarted(task.taskId(), task.taskName(), task.attempt()));
            long startTime = System.currentTimeMillis();

            @SuppressWarnings("unchecked")
            TaskHandler<Object, Object> handler = (TaskHandler<Object, Object>) registration.handler();
            Object result = handler.execute(params, ctx);
            completeTaskRun(result, task.runId());

            long duration = System.currentTimeMillis() - startTime;
            notifyListeners(l -> l.onTaskCompleted(task.taskId(), task.taskName(), task.attempt(), duration));

        } catch (SuspendTaskException e) {
            notifyListeners(l -> l.onTaskSuspended(task.taskId(), task.taskName(), task.attempt()));
        } catch (CancelledTaskException | FailedTaskException e) {
            // expected control flow
        } catch (Exception e) {
            log.error("[absurd] Task execution failed: {}", taskLabel, e);
            notifyListeners(l -> l.onTaskFailed(task.taskId(), task.taskName(), task.attempt(), 0, e));
            try {
                jdbi.useHandle(h -> failTaskRun(h, task.runId(), e));
            } catch (CancelledTaskException | FailedTaskException ignored) {}
        }
    }

    private void completeTaskRun(Object result, String runId) {
        String resultJson = JsonValue.fromObject(result).toJson();
        try {
            jdbi.useHandle(h ->
                h.createUpdate("SELECT absurd.complete_run(:queue, :runId::uuid, :result::jsonb)")
                    .bind("queue", queueName)
                    .bind("runId", runId)
                    .bind("result", resultJson)
                    .execute()
            );
        } catch (Exception e) {
            throw mapTaskStateError(e);
        }
    }

    // --- Worker ---

    public Worker startWorker(WorkerOptions options) {
        var impl = new WorkerImpl(this, options);
        this.worker = impl;
        impl.start();
        return impl;
    }

    public Worker startWorker() {
        return startWorker(WorkerOptions.defaults());
    }

    // --- Task Execution ---

    void executeTask(Handle h, ClaimedTask task, int claimTimeout) {
        executeTask(h, task, claimTimeout, leaseSeconds -> {});
    }

    void executeTask(Handle h, ClaimedTask task, int claimTimeout, java.util.function.IntConsumer onLeaseExtended) {
        var registration = registry.get(task.taskName());
        String taskLabel = task.taskName() + " (" + task.taskId() + ")";

        try {
            if (registration == null) {
                try {
                    int deferSeconds = deferClaimedRun(h, task.runId(), task.runId());
                    log.warn("Claimed unknown task \"{}\"; deferred run {} by {}s",
                            task.taskName(), task.runId(), deferSeconds);
                    return;
                } catch (Exception deferErr) {
                    log.error("Failed to defer unknown task \"{}\"; failing run", task.taskName(), deferErr);
                    try {
                        failTaskRun(h, task.runId(), deferErr);
                    } catch (CancelledTaskException | FailedTaskException ignored) {
                        return;
                    }
                    return;
                }
            }

            String effectiveQueue = registration.queue() != null ? registration.queue() : queueName;
            if (!effectiveQueue.equals(queueName)) {
                throw new AbsurdException("Misconfigured task (queue mismatch)");
            }

            TaskContext ctx = TaskContext.create(h, queueName, task, claimTimeout, onLeaseExtended);

            Object params;
            if (registration.paramsType() == JsonValue.class) {
                params = task.params() != null ? JsonValue.of(task.params()) : JsonValue.ofNull();
            } else {
                params = MAPPER.treeToValue(
                        task.params() != null ? task.params() : NullNode.getInstance(),
                        registration.paramsType()
                );
            }

            notifyListeners(l -> l.onTaskStarted(task.taskId(), task.taskName(), task.attempt()));
            long startTime = System.currentTimeMillis();

            @SuppressWarnings("unchecked")
            TaskHandler<Object, Object> handler = (TaskHandler<Object, Object>) registration.handler();
            Object result = handler.execute(params, ctx);
            completeTaskRun(h, task.runId(), result);

            long duration = System.currentTimeMillis() - startTime;
            notifyListeners(l -> l.onTaskCompleted(task.taskId(), task.taskName(), task.attempt(), duration));

        } catch (SuspendTaskException e) {
            notifyListeners(l -> l.onTaskSuspended(task.taskId(), task.taskName(), task.attempt()));
        } catch (CancelledTaskException | FailedTaskException e) {
            // Task cancelled or already failed — do nothing
        } catch (Exception e) {
            log.error("[absurd] Task execution failed: {}", taskLabel, e);
            long duration = System.currentTimeMillis();
            notifyListeners(l -> l.onTaskFailed(task.taskId(), task.taskName(), task.attempt(), 0, e));
            try {
                failTaskRun(h, task.runId(), e);
            } catch (CancelledTaskException | FailedTaskException ignored) {
            }
        }
    }

    private void completeTaskRun(Handle h, String runID, Object result) {
        String resultJson = JsonValue.fromObject(result).toJson();
        try {
            h.createUpdate("SELECT absurd.complete_run(:queue, :runId::uuid, :result::jsonb)")
                    .bind("queue", queueName)
                    .bind("runId", runID)
                    .bind("result", resultJson)
                    .execute();
        } catch (Exception e) {
            throw mapTaskStateError(e);
        }
    }

    private void failTaskRun(Handle h, String runID, Exception err) {
        String failureJson = serializeError(err);
        try {
            h.createUpdate("SELECT absurd.fail_run(:queue, :runId::uuid, :failure::jsonb, NULL)")
                    .bind("queue", queueName)
                    .bind("runId", runID)
                    .bind("failure", failureJson)
                    .execute();
        } catch (Exception e) {
            throw mapTaskStateError(e);
        }
    }

    private int deferClaimedRun(Handle h, String runID, String jitterSeed) {
        int deferSeconds = UNKNOWN_TASK_DEFER_BASE_SECONDS +
                deterministicJitterSeconds(jitterSeed, UNKNOWN_TASK_DEFER_JITTER_SECONDS);
        h.createUpdate("SELECT absurd.schedule_run(:queue, :runId::uuid, absurd.current_time() + make_interval(secs => :seconds))")
                .bind("queue", queueName)
                .bind("runId", runID)
                .bind("seconds", deferSeconds)
                .execute();
        return deferSeconds;
    }

    private TaskResultSnapshot awaitTaskResultWithBackoff(String queue, String taskID, Integer timeoutSeconds) {
        Long timeoutMs = null;
        if (timeoutSeconds != null && timeoutSeconds != Integer.MAX_VALUE) {
            if (timeoutSeconds < 0) {
                throw new AbsurdException("timeout must be a non-negative number");
            }
            timeoutMs = (long) timeoutSeconds * 1000;
        }

        long startedAt = System.currentTimeMillis();
        long delayMs = 50;

        while (true) {
            TaskResultSnapshot snapshot = jdbi.withHandle(h -> fetchTaskResultSnapshot(h, queue, taskID));
            if (snapshot == null) {
                throw new AbsurdException("Task \"" + taskID + "\" not found");
            }
            if (TaskResultSnapshot.isTerminal(snapshot)) {
                return snapshot;
            }

            if (timeoutMs != null) {
                long elapsed = System.currentTimeMillis() - startedAt;
                long remaining = timeoutMs - elapsed;
                if (remaining <= 0) {
                    throw new TimeoutException("Timed out waiting for task \"" + taskID + "\"");
                }
                delayMs = Math.min(delayMs, remaining);
            }

            try {
                Thread.sleep(Math.max(0, delayMs));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AbsurdException("Interrupted while waiting for task result", e);
            }
            delayMs = Math.min(delayMs * 2, 1000);
        }
    }

    private static TaskResultSnapshot fetchTaskResultSnapshot(Handle h, String queue, String taskID) {
        var rows = h.createQuery("SELECT state, result, failure_reason FROM absurd.get_task_result(:queue, :taskId::uuid)")
                .bind("queue", queue)
                .bind("taskId", taskID)
                .mapToMap()
                .list();

        if (rows.isEmpty()) {
            return null;
        }

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
    }

    private static ObjectNode buildSpawnPayload(SpawnOptions options, int maxAttempts,
                                                CancellationPolicy cancellation) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("max_attempts", maxAttempts);

        if (options.headers() != null && !options.headers().isEmpty()) {
            node.set("headers", MAPPER.valueToTree(options.headers()));
        }
        if (options.retryStrategy() != null) {
            node.set("retry_strategy", options.retryStrategy().toJson());
        }
        if (cancellation != null) {
            node.set("cancellation", cancellation.toJson());
        }
        if (options.idempotencyKey() != null) {
            node.put("idempotency_key", options.idempotencyKey());
        }
        return node;
    }

    private static ClaimedTask mapClaimedTask(Map<String, Object> row) {
        JsonNode params = parseJsonField(row.get("params"));
        JsonNode retryStrategy = parseJsonField(row.get("retry_strategy"));
        JsonNode headers = parseJsonField(row.get("headers"));
        JsonNode eventPayload = parseJsonField(row.get("event_payload"));
        Number maxAttempts = (Number) row.get("max_attempts");

        return new ClaimedTask(
                row.get("run_id").toString(),
                row.get("task_id").toString(),
                (String) row.get("task_name"),
                ((Number) row.get("attempt")).intValue(),
                params,
                retryStrategy,
                maxAttempts != null ? maxAttempts.intValue() : null,
                headers,
                (String) row.get("wake_event"),
                eventPayload
        );
    }

    private static JsonNode parseJsonField(Object value) {
        if (value == null) {
            return NullNode.getInstance();
        }
        if (value instanceof JsonNode jn) {
            return jn;
        }
        try {
            return MAPPER.readTree(value.toString());
        } catch (Exception e) {
            return NullNode.getInstance();
        }
    }

    private static String serializeError(Exception err) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("name", err.getClass().getSimpleName());
        node.put("message", err.getMessage());
        var sw = new java.io.StringWriter();
        err.printStackTrace(new java.io.PrintWriter(sw));
        node.put("stack", sw.toString());
        return node.toString();
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

    private static String validateQueueName(String queueName) {
        if (queueName == null || queueName.isEmpty()) {
            throw new AbsurdException("Queue name must be provided");
        }
        if (queueName.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > MAX_QUEUE_NAME_LENGTH) {
            throw new AbsurdException(
                    "Queue name \"" + queueName + "\" is too long (max " + MAX_QUEUE_NAME_LENGTH + " bytes).");
        }
        return queueName;
    }

    private static int deterministicJitterSeconds(String seed, int maxJitterSeconds) {
        if (maxJitterSeconds <= 0) {
            return 0;
        }
        int hash = (int) 2166136261L;
        for (int i = 0; i < seed.length(); i++) {
            hash ^= seed.charAt(i);
            hash *= 16777619;
        }
        return Math.abs(hash) % (maxJitterSeconds + 1);
    }

    @Override
    public void close() {
        if (worker != null) {
            worker.close();
        }
    }

    private void notifyListeners(java.util.function.Consumer<TaskLifecycleListener> action) {
        for (var listener : listeners) {
            try {
                action.accept(listener);
            } catch (Exception e) {
                log.warn("TaskLifecycleListener threw exception", e);
            }
        }
    }
}
