# Feature: `spawnAndExecute` — Immediate Local Execution

**Status**: Design complete — ready for implementation  
**Last updated**: 2026-07-01

## Summary

A new method on `Absurd` that spawns a task (persisted to Postgres) and immediately
attempts to claim and execute it locally, bypassing polling latency.

## Motivation

Avoid scheduling latency for the happy path in use cases like tests where predictable
and rapid response is needed.

## Behavior

1. Task is persisted to Postgres (full durability, same as `spawn`)
2. Returns `SpawnResult` immediately (non-blocking)
3. After persisting, the local instance attempts to claim the specific task by its run ID and execute it in the background
4. Uses the same registered task handler as normal workers
5. Single-threaded — executes one task, not a general-purpose worker loop
6. **Best-effort**: if a remote worker claims the task first, the local attempt silently backs off
7. **Happy path optimization**: if execution fails, retries follow the normal scheduled retry path (no local retry)
8. **Suspend/sleep**: if the task suspends (event/sleep), it suspends normally — resumed later by any worker

## Coexistence

- Works alongside normal polling workers on the same queue
- Same or different `Absurd` instance — doesn't matter
- No special reservation or locking — uses normal `FOR UPDATE SKIP LOCKED` semantics

## API

```java
SpawnResult result = absurd.spawnAndExecute("task-name", params);
SpawnResult result = absurd.spawnAndExecute("task-name", params, spawnOptions);
```

## Design Decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | Scope | Local worker mode that processes locally-spawned tasks |
| 2 | Durability | Persisted to Postgres before execution |
| 3 | Return value | `SpawnResult` (non-blocking), same as `spawn()` |
| 4 | Retry on failure | Falls back to normal scheduled retry path |
| 5 | Coexistence | Best-effort; if another worker claims it first, silently back off |
| 6 | Event/sleep | Same as today, no special handling |
| 7 | Scope of local | Only executes the specific task just spawned |
| 8 | Concurrency | Single execution, no concurrency |
| 9 | Activation | Dedicated method: `spawnAndExecute(...)` |
| 10 | Claiming mechanism | New SQL function `claim_specific_task` targeting a specific `run_id` |

## Design: New SQL Function

### `absurd.claim_specific_task`

```sql
create function absurd.claim_specific_task(
  p_queue_name text,
  p_run_id uuid,
  p_worker_id text,
  p_claim_timeout integer
)
  returns table (
    run_id uuid,
    task_id uuid,
    attempt integer,
    task_name text,
    params jsonb,
    retry_strategy jsonb,
    max_attempts integer,
    headers jsonb,
    wake_event text,
    event_payload jsonb
  )
  language plpgsql
as $$
declare
  v_now timestamptz := absurd.current_time();
  v_claim_until timestamptz := v_now + make_interval(secs => p_claim_timeout);
begin
  return query execute format(
    'with candidate as (
        select r.run_id
          from absurd.%1$I r
          join absurd.%2$I t on t.task_id = r.task_id
         where r.run_id = $1
           and r.state in (''pending'', ''sleeping'')
           and t.state in (''pending'', ''sleeping'', ''running'')
           and r.available_at <= $2
         for update skip locked
     ),
     updated as (
        update absurd.%1$I r
           set state = ''running'',
               claimed_by = $3,
               claim_expires_at = $4,
               started_at = $2,
               available_at = $2
         where run_id in (select run_id from candidate)
         returning r.run_id, r.task_id, r.attempt
     ),
     task_upd as (
        update absurd.%2$I t
           set state = ''running'',
               attempts = greatest(t.attempts, u.attempt),
               first_started_at = coalesce(t.first_started_at, $2),
               last_attempt_run = u.run_id
          from updated u
         where t.task_id = u.task_id
         returning t.task_id
     )
     select
       u.run_id,
       u.task_id,
       u.attempt,
       t.task_name,
       t.params,
       t.retry_strategy,
       t.max_attempts,
       t.headers,
       r.wake_event,
       r.event_payload
     from updated u
     join absurd.%1$I r on r.run_id = u.run_id
     join absurd.%2$I t on t.task_id = u.task_id',
    'r_' || p_queue_name,
    't_' || p_queue_name
  ) using p_run_id, v_now, p_worker_id, v_claim_until;
end;
$$;
```

**Key differences from `claim_task`:**
- Targets a specific `run_id` instead of FIFO ordering
- No cancellation sweep or expired lease sweep (handled by regular worker's `claim_task` calls)
- No batch — returns 0 or 1 row
- Same state transitions and locking semantics (`FOR UPDATE SKIP LOCKED`)

## Java SDK Implementation

### Flow

```
spawnAndExecute(taskName, params, options):
  1. result = spawn(taskName, params, options)       // persist to Postgres
  2. In a background thread:
     a. claimed = claimSpecificTask(queue, result.runID, "local:<pid>", claimTimeout)
     b. if claimed is empty → silently back off (already claimed by another worker)
     c. if claimed is present → executeTaskPooled(claimed)
  3. Return result immediately (step 2 is async)
```

### Flow Diagram

```
Caller                     Postgres                    Background Thread
  │                           │                              │
  │── spawn(task) ───────────►│ (INSERT, returns SpawnResult)│
  │◄── SpawnResult ───────────│                              │
  │                           │                              │
  │── return SpawnResult ─►   │                              │
  │                           │◄── claim_specific_task() ────│
  │                           │── claimed (or empty) ────────►│
  │                           │                              │
  │                           │   [if claimed: execute handler]
  │                           │◄── complete_run() ───────────│
  │                           │                              │
```

### New Methods on `Absurd.java`

```java
public SpawnResult spawnAndExecute(String taskName, Object params) {
    return spawnAndExecute(taskName, params, SpawnOptions.defaults());
}

public SpawnResult spawnAndExecute(String taskName, Object params, SpawnOptions options) {
    SpawnResult result = spawn(taskName, params, options);

    String queue = resolveQueue(taskName, options);
    Thread.startVirtualThread(() -> {
        try {
            List<ClaimedTask> claimed = claimSpecificTask(queue, result.runID(), 120, "local-exec");
            if (!claimed.isEmpty()) {
                executeTaskPooled(claimed.get(0), 120);
            }
        } catch (Exception e) {
            log.debug("[absurd] spawnAndExecute local claim failed (best-effort): {}", e.getMessage());
        }
    });

    return result;
}
```

### New SQL Binding on `Absurd.java`

```java
List<ClaimedTask> claimSpecificTask(String queue, String runId, int claimTimeout, String workerId) {
    return jdbi.withHandle(h -> {
        var rows = h.createQuery(
                "SELECT run_id, task_id, attempt, task_name, params, retry_strategy, " +
                "max_attempts, headers, wake_event, event_payload " +
                "FROM absurd.claim_specific_task(:queue, :runId::uuid, :workerId, :claimTimeout)")
            .bind("queue", queue)
            .bind("runId", runId)
            .bind("workerId", workerId)
            .bind("claimTimeout", claimTimeout)
            .mapToMap()
            .list();

        List<ClaimedTask> tasks = new ArrayList<>();
        for (var row : rows) {
            tasks.add(mapClaimedTask(row));
        }
        return tasks;
    });
}
```

## Properties

| Aspect | Behavior |
|--------|----------|
| Durability | Task persisted before execution attempt |
| Return | `SpawnResult` returned immediately (non-blocking) |
| Claiming | Targets only our specific `run_id` — no side effects on other tasks |
| Race lost | Returns empty → silent back-off, task runs via normal worker |
| Failure | `executeTaskPooled` handles it → `fail_run` → normal retry schedule |
| Suspend | `SuspendTaskException` caught → task sleeps, resumed later by any worker |
| Concurrency | Single background thread per `spawnAndExecute` call |
| Worker coexistence | Normal workers unaffected — they use `claim_task` as before |

## Acceptance Tests

### Happy Path

1. **Task spawns and executes locally** — `spawnAndExecute` persists the task, claims it, and the handler runs to completion. `awaitTaskResult` returns `Completed` with the expected result.

2. **Returns SpawnResult immediately** — `spawnAndExecute` returns before the handler finishes executing. Verify the returned `SpawnResult` has valid `taskID`, `runID`, `attempt=1`, `created=true`.

3. **Uses the registered handler** — Register a handler that records invocation (e.g. writes to an `AtomicReference`). Call `spawnAndExecute`. Verify the handler was invoked with the correct params.

### Durability & Persistence

4. **Task is persisted before execution** — Register a handler that blocks on a latch. Call `spawnAndExecute`. Before releasing the latch, verify the task exists in the database (via `fetchTaskResult` returning `Running` or `Pending`).

5. **Steps are checkpointed** — Handler uses `ctx.step(...)`. After completion, verify the task result reflects the step outputs (same as a normal worker execution).

### Failure & Retry

6. **Handler failure falls back to normal retry** — Register a handler that throws on first attempt. Call `spawnAndExecute`. Verify the task enters the retry schedule (state becomes `Pending`/`Sleeping` with a future `available_at`). Start a normal worker — it picks up and retries the task.

7. **No local retry on failure** — Register a handler that counts invocations and always throws. Call `spawnAndExecute`. Wait briefly. Verify the handler was invoked exactly once locally.

### Best-Effort / Race Condition

8. **Silently backs off if task already claimed** — Spawn a task normally with `spawn()`, immediately claim it with a worker (or `workBatch`). Then verify that if `spawnAndExecute` were in that scenario, it doesn't crash or throw — it just returns the `SpawnResult` and the task runs elsewhere.

9. **Works when queue has other pending tasks** — Spawn several tasks normally, then call `spawnAndExecute` for a new task. The new task should still execute (eventually, either locally or by a worker). No tasks should be lost.

### Coexistence with Workers

10. **Coexists with a running worker** — Start a normal worker. Call `spawnAndExecute`. The task completes (either locally or via the worker). No errors, no double-execution.

11. **No double execution** — Call `spawnAndExecute` while a worker is also running. Verify the handler is invoked exactly once (not by both local and remote).

### Suspend / Sleep

12. **Task that suspends on awaitEvent** — Register a handler that calls `ctx.awaitEvent(...)`. Call `spawnAndExecute`. The local execution suspends (task becomes `Sleeping`). Emit the event, start a worker — the task resumes and completes.

13. **Task that calls sleepFor** — Register a handler that calls `ctx.sleepFor(...)`. Call `spawnAndExecute`. The local execution suspends normally. Task completes once a worker picks it up after the sleep elapses.

### Edge Cases

14. **Unregistered task name** — Call `spawnAndExecute` with a task name that has no registered handler. Verify it throws `AbsurdException` (same as `spawn` behavior for unregistered tasks without a queue).

15. **SpawnOptions are respected** — Call `spawnAndExecute` with custom `SpawnOptions` (maxAttempts, headers, idempotencyKey). Verify the persisted task reflects these options and the handler receives the correct headers via `ctx.headers()`.

16. **Idempotency key deduplication** — Call `spawnAndExecute` twice with the same idempotency key. Second call returns `created=false`. Handler runs at most once.

## Files to Change

| File | Change |
|------|--------|
| `sql/absurd.sql` | Add `absurd.claim_specific_task` function |
| `sdks/java_sdk/src/main/java/io/absurd/sdk/Absurd.java` | Add `spawnAndExecute` (2 overloads) + `claimSpecificTask` binding |
| `sdks/java_sdk/src/test/java/io/absurd/sdk/SpawnAndExecuteTest.java` | New test class with acceptance tests |

## Implementation Notes

- `Absurd.java` already has `executeTaskPooled(ClaimedTask, claimTimeout)` which handles
  the full execution lifecycle (handler lookup, context creation, completion/failure)
- Background execution uses a virtual thread (`Thread.startVirtualThread`) — lightweight,
  no thread pool needed since it's single-execution
- Claim timeout for local execution can use a sensible default (e.g. 120s)
- The `resolveQueue` helper needs to be extracted from the existing `spawn` logic to avoid duplication
