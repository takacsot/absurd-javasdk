# Absurd Java SDK

Java SDK for [Absurd](https://github.com/earendil-works/absurd) — PostgreSQL-native durable task execution.

Uses [JDBI](https://jdbi.org/) for database access.

## Requirements

- Java 17+
- PostgreSQL with the Absurd schema applied

## Installation

> **🚧 Work in Progress**

## Quick Start

```java
import io.absurd.sdk.*;
import javax.sql.DataSource;

// Create client
var absurd = Absurd.create(dataSource, "default");
absurd.createQueue();

// Register a task
absurd.registerTask(TaskRegistration.builder("order-fulfillment")
    .handler(OrderParams.class, (params, ctx) -> {
        var payment = ctx.step("process-payment", PaymentResult.class, () -> {
            return new PaymentResult("pay-" + params.orderId(), params.amount());
        });

        var inventory = ctx.step("reserve-inventory", ReserveResult.class, () -> {
            return new ReserveResult(params.items());
        });

        // Wait for external event
        var shipment = ctx.awaitEvent("shipment.packed:" + params.orderId());

        return Map.of(
            "orderId", params.orderId(),
            "trackingNumber", shipment.node().get("trackingNumber").asText()
        );
    })
    .build());

// Start a worker
var worker = absurd.startWorker(WorkerOptions.builder()
    .concurrency(4)
    .build());

// Spawn a task
absurd.spawn("order-fulfillment", new OrderParams("42", 9999, List.of("widget-1")));

// Emit an event
absurd.emitEvent("shipment.packed:42", Map.of("trackingNumber", "TRACK123"));

// Clean up
worker.close();
absurd.close();
```

## Core Concepts

### Tasks and Steps

Tasks are the unit of work. Steps are checkpointed operations within a task — if the
process crashes, execution resumes from the last completed step.

```java
absurd.registerTask(TaskRegistration.builder("my-task")
    .defaultMaxAttempts(3)
    .handler(MyParams.class, (params, ctx) -> {
        // This step runs once; result is cached on retries
        var data = ctx.step("fetch-data", Data.class, () -> fetchFromApi());

        // Multiple steps in sequence
        var processed = ctx.step("process", Result.class, () -> process(data));

        return processed;
    })
    .build());
```

### Events

Tasks can wait for named events. Events use first-write-wins semantics.

```java
// In a task handler:
JsonValue payload = ctx.awaitEvent("order:123:shipped");
JsonValue payload = ctx.awaitEvent("order:123:shipped", 30); // with 30s timeout

// From outside:
absurd.emitEvent("order:123:shipped", Map.of("carrier", "fedex"));
```

### Sleep

Tasks can suspend for a duration or until a specific time.

```java
ctx.sleepFor("cooldown", Duration.ofMinutes(5));
ctx.sleepUntil("scheduled", Instant.parse("2025-01-01T00:00:00Z"));
```

### Workers

Workers poll for tasks and execute them with configurable concurrency.

```java
var worker = absurd.startWorker(WorkerOptions.builder()
    .workerId("my-service:1")
    .concurrency(8)
    .claimTimeout(120)
    .pollIntervalSeconds(0.25)
    .onError(ex -> log.error("Worker error", ex))
    .build());

// Graceful shutdown
worker.close();
```

## Running Tests

```bash
./gradlew test
```

Tests use [embedded PostgreSQL](https://github.com/zonkyio/embedded-postgres) — no Docker required.

## SDK Usage

### Creating an Absurd Client

**Factory methods** (quick setup):

```java
// Minimal — uses "default" queue, 5 max attempts
var absurd = Absurd.create(dataSource);

// Custom queue name
var absurd = Absurd.create(dataSource, "orders");

// Custom queue + max attempts
var absurd = Absurd.create(dataSource, "orders", 10);

// From an existing JDBI instance
var absurd = Absurd.create(jdbi, "orders");
```

**Builder** (full control):

```java
var absurd = Absurd.builder(dataSource)
    .queueName("orders")
    .defaultMaxAttempts(10)
    .listener(new TaskLifecycleListener() {
        @Override
        public void onTaskStarted(String taskId, String taskName, int attempt) {
            log.info("Task {} started (attempt {})", taskName, attempt);
        }
        @Override
        public void onTaskFailed(String taskId, String taskName, int attempt, long durationMs, Exception error) {
            log.error("Task {} failed after {}ms", taskName, durationMs, error);
        }
    })
    .build();
```

| Parameter | Default | Description |
|---|---|---|
| `queueName` | `"default"` | Default queue for all operations |
| `defaultMaxAttempts` | `5` | Max retries before permanent failure |
| `listener` | none | Lifecycle callbacks (can add multiple) |

### Registering Tasks

**Full builder:**

```java
absurd.registerTask(TaskRegistration.builder("send-email")
    .queue("notifications")              // override client's default queue
    .defaultMaxAttempts(3)               // override client's max attempts
    .defaultCancellation(CancellationPolicy.of(
        300,   // maxDuration: cancel if running > 5 minutes
        60     // maxDelay: cancel if not started within 60 seconds
    ))
    .handler(EmailParams.class, (params, ctx) -> {
        // task logic
        return sendEmail(params);
    })
    .build());
```

**Shorthand** (name + handler only):

```java
absurd.registerTask("ping", Void.class, (params, ctx) -> {
    ctx.step("pong", () -> "ok");
    return null;
});
```

| Parameter | Required | Description |
|---|---|---|
| `name` | yes | Unique task name used for spawn/routing |
| `handler` | yes | `TaskHandler<P, R>` implementation |
| `queue` | no | Overrides client queue; spawn must match |
| `defaultMaxAttempts` | no | Overrides client default |
| `defaultCancellation` | no | Auto-cancel policy for this task type |

### Spawning Tasks

**Simple:**

```java
SpawnResult result = absurd.spawn("send-email", new EmailParams("user@example.com", "Hello"));
// result.taskID()  — unique task identifier
// result.runID()   — current run identifier
// result.attempt() — attempt number (1 for first)
// result.created() — false if deduplicated by idempotency key
```

**With options:**

```java
SpawnResult result = absurd.spawn("send-email", params, SpawnOptions.builder()
    .maxAttempts(10)
    .retryStrategy(RetryStrategy.exponential(1.0, 2.0, 60.0))
    .headers(Map.of("tenant", "acme", "priority", "high"))
    .idempotencyKey("email-user123-welcome")
    .cancellation(CancellationPolicy.of(600, null))
    .build());
```

**Within an existing transaction** (using JDBI Handle or JDBC Connection):

```java
jdbi.useHandle(h -> {
    // Spawn as part of a larger transaction
    absurd.spawn(h, "process-order", orderParams);
    h.execute("UPDATE orders SET status = 'queued' WHERE id = ?", orderId);
});

// Or with raw JDBC Connection:
absurd.spawn(connection, "process-order", orderParams);
```

| SpawnOptions field | Default | Description |
|---|---|---|
| `maxAttempts` | from registration or client | Max execution attempts |
| `retryStrategy` | from registration | How retries are scheduled |
| `headers` | none | Metadata accessible via `ctx.headers()` in handler |
| `queue` | from registration | Must match registered queue if set |
| `cancellation` | from registration | Per-task cancellation policy |
| `idempotencyKey` | none | Deduplicates spawns; same key = same task |

### Retry Strategies

```java
// Fixed delay between retries
RetryStrategy.fixed(5.0)  // 5 seconds between each retry

// Exponential backoff
RetryStrategy.exponential(1.0, 2.0, 60.0)
// base=1s, factor=2x, max=60s → delays: 1s, 2s, 4s, 8s, ... 60s

// No retries
RetryStrategy.none()
```

### Cancellation Policies

```java
// Cancel if not started within 30 seconds
CancellationPolicy.of(null, 30)

// Cancel if running longer than 5 minutes
CancellationPolicy.of(300, null)

// Both constraints
CancellationPolicy.of(300, 30)
```

| Field | Description |
|---|---|
| `maxDuration` | Seconds after first execution start; cancels long-running tasks |
| `maxDelay` | Seconds after enqueue; cancels tasks stuck in queue |

### Task Results

**Non-blocking poll:**

```java
TaskResultSnapshot snapshot = absurd.fetchTaskResult(taskId);

switch (snapshot) {
    case TaskResultSnapshot.Completed c -> System.out.println("Result: " + c.result());
    case TaskResultSnapshot.Failed f    -> System.out.println("Error: " + f.failure());
    case TaskResultSnapshot.Cancelled c -> System.out.println("Cancelled");
    case TaskResultSnapshot.Running r   -> System.out.println("Still running");
    case TaskResultSnapshot.Sleeping s  -> System.out.println("Sleeping/waiting");
    case TaskResultSnapshot.Pending p   -> System.out.println("Not started yet");
}
```

**Blocking wait:**

```java
// Wait indefinitely
TaskResultSnapshot result = absurd.awaitTaskResult(taskId);

// Wait with timeout (throws TimeoutException)
TaskResultSnapshot result = absurd.awaitTaskResult(taskId, "orders", 30);
```

**Cross-queue lookup:**

```java
TaskResultSnapshot result = absurd.fetchTaskResult(taskId, "other-queue");
```

### Cancelling and Retrying Tasks

```java
// Cancel a task
absurd.cancelTask(taskId);
absurd.cancelTask(taskId, "specific-queue");

// Retry a failed/cancelled task
SpawnResult retry = absurd.retryTask(taskId);

// Retry with new max attempts, or spawn a brand new run
SpawnResult retry = absurd.retryTask(taskId, "orders", 10, false);
SpawnResult fresh = absurd.retryTask(taskId, "orders", null, true); // spawnNew=true
```

### Emitting Events

```java
// Simple event (no payload)
absurd.emitEvent("order:123:shipped");

// Event with payload
absurd.emitEvent("order:123:shipped", Map.of("carrier", "fedex", "tracking", "TR-99"));

// To a specific queue
absurd.emitEvent("order:123:shipped", payload, "orders");
```

### Queue Management

```java
// Create using client's default queue name
absurd.createQueue();

// Create a named queue
absurd.createQueue("notifications");

// Create with storage mode
absurd.createQueue("analytics", "unpartitioned");

// List all queues
List<String> queues = absurd.listQueues();

// Drop a queue (deletes all data!)
absurd.dropQueue("old-queue");
```

### Workers

**Start with options:**

```java
Worker worker = absurd.startWorker(WorkerOptions.builder()
    .workerId("my-service:1")
    .concurrency(8)
    .batchSize(4)
    .claimTimeout(120)
    .pollIntervalSeconds(0.5)
    .shutdownTimeoutSeconds(60)
    .fatalOnLeaseTimeout(true)
    .onError(ex -> log.error("Worker error", ex))
    .build());
```

**Start with defaults:**

```java
Worker worker = absurd.startWorker(); // concurrency=1, poll=0.25s
```

| WorkerOptions field | Default | Description |
|---|---|---|
| `workerId` | `hostname:pid` | Identifies worker in database |
| `concurrency` | `1` | Parallel task execution threads |
| `batchSize` | same as `concurrency` | Tasks claimed per poll cycle |
| `claimTimeout` | `120` | Seconds before lease expires |
| `pollIntervalSeconds` | `0.25` | Seconds between empty polls |
| `shutdownTimeoutSeconds` | `30` | Grace period on `close()` |
| `fatalOnLeaseTimeout` | `true` | Treat expired leases as fatal |
| `onError` | no-op | Error callback for worker-level failures |

**Graceful shutdown with a JVM shutdown hook:**

```java
Worker worker = absurd.startWorker(WorkerOptions.builder()
    .concurrency(10)
    .pollIntervalSeconds(3)
    .shutdownTimeoutSeconds(10)
    .build());

Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    System.out.println("Shutting down worker...");
    worker.close();
    absurd.close();
    System.out.println("Worker stopped.");
}));

// Block main thread; Ctrl+C triggers graceful shutdown
Thread.currentThread().join();
```

### Manual Batch Processing

For control over when and how tasks are processed (useful in tests or single-threaded apps):

```java
// Handle-bound: holds one connection for entire batch
absurd.workBatch("worker-1", 30, 5);

// Pooled: acquires/releases connections per SQL call
absurd.workBatchPooled("worker-1", 30, 5);
```

### Lifecycle Listeners

```java
absurd = Absurd.builder(dataSource)
    .listener(new TaskLifecycleListener() {
        @Override
        public void onTaskRegistered(String taskName) { /* ... */ }

        @Override
        public void onTaskStarted(String taskId, String taskName, int attempt) { /* ... */ }

        @Override
        public void onTaskCompleted(String taskId, String taskName, int attempt, long durationMs) { /* ... */ }

        @Override
        public void onTaskFailed(String taskId, String taskName, int attempt, long durationMs, Exception error) { /* ... */ }

        @Override
        public void onTaskSuspended(String taskId, String taskName, int attempt) { /* ... */ }
    })
    .build();
```

All callbacks are optional (interface uses default methods).

## Deep Dive

### Architecture Overview

The SDK has three layers:

1. **Client (`Absurd`)** — Coordinates task registration, spawning, event emission, and worker lifecycle. Holds a JDBI instance and the task registry.
2. **Worker (`WorkerImpl`)** — A background poller that claims tasks and dispatches them to a thread pool.
3. **Context (`TaskContext`)** — Passed to each handler invocation; provides step checkpointing, sleep, events, and heartbeat backed by PostgreSQL functions.

All durable state lives in PostgreSQL. The SDK is stateless beyond in-memory task registration and the worker's thread pool.

### Claiming and Leasing

Workers acquire tasks via `absurd.claim_task(queue, worker_id, claim_timeout, qty)`:

- Uses `FOR UPDATE SKIP LOCKED` to prevent double-claiming across concurrent workers.
- Tasks are claimed in FIFO order (`ORDER BY available_at, run_id`).
- A claimed task's `claim_expires_at` is set to `now() + claim_timeout`.
- If a worker doesn't complete or heartbeat before expiry, the next `claim_task` call detects the expired lease and fails the run with a `$ClaimTimeout` error, which triggers retry logic.

The claim flow in the SDK:

```
WorkerImpl.pollLoop()
  → absurd.claimTasks(batchSize, claimTimeout, workerId)
    → SQL: absurd.claim_task(queue, worker, timeout, qty)
      → cancellation sweep (max_delay / max_duration)
      → expired lease sweep (fail timed-out runs)
      → claim pending/sleeping runs with available_at <= now()
```

### Worker Lifecycle

`startWorker()` creates a `WorkerImpl` with:

- A **poller thread** (daemon) running `pollLoop()`
- A **fixed thread pool** of size `concurrency`
- A **semaphore** of size `concurrency` for backpressure

The poll loop:

1. Checks available semaphore permits.
2. If zero permits, blocks until one is released (a task completes).
3. Claims `min(batchSize, availablePermits)` tasks.
4. If no tasks available, sleeps for `pollIntervalSeconds`.
5. For each claimed task, acquires a permit and submits execution to the thread pool.

**Shutdown** (`worker.close()`):

1. Sets `running = false` and interrupts the poller thread.
2. Calls `executor.shutdown()` — no new tasks accepted.
3. Waits up to `shutdownTimeoutSeconds` for in-progress tasks to finish.
4. Force-shuts down if timeout elapses.

Tasks still running at forced shutdown will eventually have their leases expire and be retried by another worker.

### Step Idempotency and Retries

Steps are checkpointed to PostgreSQL via `absurd.set_task_checkpoint_state()`. On retry:

1. `TaskContext` loads all existing checkpoints for the task at creation time.
2. When `ctx.step("name", ...)` is called, it first checks the in-memory cache.
3. If a checkpoint exists, the cached result is returned — the `Callable` is **not** re-executed.
4. If no checkpoint exists, the callable runs, the result is persisted, and the lease is extended.

This guarantees at-most-once execution per step across retries. The step name is the idempotency key, with automatic `#2`, `#3` suffixes for repeated names.

When a task fails (unhandled exception), `fail_run` is called which:
- Records the error
- Computes the next retry delay based on the task's `retry_strategy` (exponential backoff, fixed, etc.)
- Schedules a new run with `available_at` in the future

### Event System

**`awaitEvent(eventName)`** suspends task execution until a named event arrives:

1. Calls `absurd.await_event(queue, task_id, run_id, checkpoint, event, timeout)`.
2. If the event already exists, returns its payload immediately (persisted as a checkpoint).
3. If not, the SQL function registers a wait record and the SDK throws `SuspendTaskException`.
4. `SuspendTaskException` is caught by the execution loop — the task is left in `sleeping` state without failing.

**`emitEvent(eventName, payload)`** wakes sleeping tasks:

1. Calls `absurd.emit_event(queue, event, payload)`.
2. The SQL function uses first-write-wins: if the event already exists, the emit is a no-op.
3. Any task waiting on this event has its run rescheduled with `available_at = now()`.
4. On the next poll cycle, the worker claims the resumed task. The `awaitEvent` step finds the checkpoint and returns the payload.

Events with a timeout: if the timeout elapses, the wait record's `timeout_at` fires, the task resumes, and `awaitEvent` throws `TimeoutException`.

### Connection Management

The SDK supports two execution modes:

**Handle-bound** (`TaskContext.create(handle, ...)`) — used by `workBatch()`:
- A single JDBC connection is held for the entire task execution.
- All checkpoints, heartbeats, and completion happen on the same connection.
- Simpler transactional semantics but holds a connection from the pool for the task's duration.

**Pooled** (`TaskContext.createPooled(jdbi, ...)`) — used by `workBatchPooled()` and the default worker:
- No connection is held while user code executes.
- Each SQL operation (checkpoint, heartbeat, complete) acquires and releases a connection independently.
- Better for long-running tasks or high concurrency — connections are not tied up during computation.

The worker's `executeTask` uses handle-bound mode. Choose pooled mode explicitly via `workBatchPooled()` if your tasks are long-running or you need to maximize connection utilization.

### Unit Testing Handlers

The SDK provides `TestTaskContext` — a passthrough implementation of `TaskOperations` that requires no database - test your business logic independently:

```java
var ctx = TestTaskContext.builder()
    .taskId("test-1")
    .headers(Map.of("tenant", "acme"))
    .eventResponse("order.shipped", Map.of("tracking", "TR-1"))
    .build();

var result = myHandler.execute(params, ctx);

// Assert step results
assertThat(ctx.getStepResults()).containsKey("process-payment");

// Assert emitted events
assertThat(ctx.getEmittedEvents()).hasSize(1);
```

Steps execute immediately (no caching/replay). Sleep and heartbeat are no-ops. Events return pre-configured responses or throw `TimeoutException` if unconfigured.
