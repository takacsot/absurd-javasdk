# Dynamic Polling Interval — Specification

## Summary

Add adaptive polling to the Java SDK worker. When there is work to do, poll
aggressively (down to 25ms). When the queue is idle, linearly back off up to 5s.
This reduces unnecessary database load during quiet periods while keeping latency
low under load.

## Current Behavior

The worker polls at a fixed `pollIntervalSeconds` (default 0.25s) regardless of
queue activity. Empty polls and busy polls sleep the same amount.

## Proposed Behavior

When **dynamic polling** is enabled (opt-in):

1. **Busy → poll fast**: After a successful claim that returned tasks, reset the
   interval to `minPollInterval`.
2. **Empty → back off linearly**: After an empty poll (zero tasks claimed),
   increase the interval by a fixed step until `maxPollInterval` is reached.
3. **Batch saturation awareness**: If the number of claimed tasks equals the
   requested batch size (full batch), do NOT sleep at all — immediately re-poll.
   If claimed < batchSize but > 0, reset to `minPollInterval` (tasks exist but
   queue is draining). If claimed == 0, apply linear backoff.
4. **Snap-back on activity**: Any non-empty claim instantly resets the interval
   to `minPollInterval`. No gradual ramp-up.

## Configuration

### New fields on `WorkerOptions.Builder`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `dynamicPolling` | `boolean` | `false` | Enable adaptive polling. When `false`, behavior is identical to today (fixed `pollIntervalSeconds`). |
| `minPollInterval` | `Duration` | `25ms` | Fastest poll rate when queue is busy. |
| `maxPollInterval` | `Duration` | `5s` | Slowest poll rate when queue is idle. |
| `pollBackoffStep` | `Duration` | `250ms` | Amount added to the current interval after each empty poll (linear increase). |
| `onPollIntervalChanged` | `Consumer<Duration>` | `null` | Optional callback invoked when the effective interval changes. Receives the new interval. Useful for metrics/observability. |

### Backward Compatibility

- When `dynamicPolling = false` (default), the worker uses the existing fixed
  `pollIntervalSeconds`. No behavior change for existing users.
- `pollIntervalSeconds` remains in the API and is honored when dynamic polling
  is off. When dynamic polling is on, it is ignored (min/max take precedence).
- The `WorkerOptions` record gains new fields with defaults that preserve current
  behavior.

## Algorithm

```
currentInterval = minPollInterval

loop:
  claimed = claimTasks(batchSize)

  if claimed == batchSize:
      // Full batch — queue is hot, don't sleep
      currentInterval = minPollInterval
      notify callback if interval changed
      continue (no sleep)

  if claimed > 0 and claimed < batchSize:
      // Partial batch — work exists but queue is draining
      currentInterval = minPollInterval
      notify callback if interval changed
      sleep(currentInterval)

  if claimed == 0:
      // Empty — back off
      previousInterval = currentInterval
      currentInterval = min(currentInterval + pollBackoffStep, maxPollInterval)
      notify callback if interval changed
      sleep(currentInterval)
```

## Observability

- `onPollIntervalChanged` callback fires on every transition (not on every poll).
  It only fires when the effective interval actually changes value.
- Standard SLF4J debug logging when interval changes:
  `[absurd] Poll interval changed: {}ms → {}ms`

## Edge Cases

| Scenario | Behavior |
|----------|----------|
| Worker starts up | Begin at `minPollInterval` |
| Error during claim | Apply same backoff as empty poll (protects DB during failures) |
| All concurrency slots occupied | Block on semaphore as today; interval is not affected |
| `minPollInterval` > `maxPollInterval` | Reject at build time with `IllegalArgumentException` |
| `pollBackoffStep` ≤ 0 | Reject at build time |

## Implementation Plan

1. Add new fields to `WorkerOptions` record and `Builder`.
2. Add validation in `Builder.build()`.
3. Introduce a small `PollIntervalController` internal class encapsulating the
   state machine (current interval, step logic, callback notification).
4. Modify `WorkerImpl.pollLoop()` to delegate sleep duration to the controller.
5. Add unit tests for the controller in isolation (no DB needed).
6. Add integration test: spawn tasks → verify worker polls fast → let queue
   drain → verify interval increases → spawn again → verify snap-back.

## Example Usage

```java
var worker = absurd.startWorker(WorkerOptions.builder()
    .concurrency(4)
    .dynamicPolling(true)
    .minPollInterval(Duration.ofMillis(25))
    .maxPollInterval(Duration.ofSeconds(5))
    .pollBackoffStep(Duration.ofMillis(250))
    .onPollIntervalChanged(interval ->
        log.info("Poll interval now: {}ms", interval.toMillis()))
    .build());
```

## Out of Scope

- Other SDKs (TypeScript, Python, Go) — may follow later.
- Postgres LISTEN/NOTIFY push-based wakeup (orthogonal optimization).
- Exponential backoff (decision: linear is simpler and more predictable).
