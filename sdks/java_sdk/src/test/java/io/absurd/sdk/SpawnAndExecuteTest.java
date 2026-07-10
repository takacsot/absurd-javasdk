package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpawnAndExecuteTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("sae_test_q");
    }

    @AfterAll
    static void teardown() throws Exception {
        teardownBase();
    }

    @AfterEach
    void cleanupTasks() {
        truncateQueue();
    }

    // --- Happy Path ---

    @Test
    void taskSpawnsAndExecutesLocally() throws Exception {
        absurd.registerTask("sae-simple", JsonValue.class, (params, ctx) -> {
            return params.node().get("x").asInt() * 2;
        });

        SpawnResult result = absurd.spawnAndExecute("sae-simple", Map.of("x", 21));

        var snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asInt()).isEqualTo(42);
    }

    @Test
    void returnsSpawnResultImmediately() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask("sae-immediate", JsonValue.class, (params, ctx) -> {
            latch.await(10, TimeUnit.SECONDS);
            return "done";
        });

        long start = System.currentTimeMillis();
        SpawnResult result = absurd.spawnAndExecute("sae-immediate", null);
        long elapsed = System.currentTimeMillis() - start;

        // Should return almost immediately (before handler finishes)
        assertThat(elapsed).isLessThan(2000);
        assertThat(result.taskID()).isNotNull();
        assertThat(result.runID()).isNotNull();
        assertThat(result.attempt()).isEqualTo(1);
        assertThat(result.created()).isTrue();

        latch.countDown();
        // Wait for completion
        absurd.awaitTaskResult(result.taskID(), queueName, 10);
    }

    @Test
    void usesRegisteredHandler() throws Exception {
        AtomicReference<Object> captured = new AtomicReference<>();

        absurd.registerTask("sae-handler", JsonValue.class, (params, ctx) -> {
            captured.set(params.node().get("msg").asText());
            return "ok";
        });

        SpawnResult result = absurd.spawnAndExecute("sae-handler", Map.of("msg", "hello-world"));
        absurd.awaitTaskResult(result.taskID(), queueName, 10);

        assertThat(captured.get()).isEqualTo("hello-world");
    }

    // --- Durability & Persistence ---

    @Test
    void taskIsPersistedBeforeExecution() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask("sae-persist", JsonValue.class, (params, ctx) -> {
            latch.await(10, TimeUnit.SECONDS);
            return "done";
        });

        SpawnResult result = absurd.spawnAndExecute("sae-persist", null);

        // Before releasing latch, verify task exists in DB
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isNotNull();
        // Should be pending or running (depending on timing)
        assertThat(snapshot).isNotInstanceOf(TaskResultSnapshot.Completed.class);
        assertThat(snapshot).isNotInstanceOf(TaskResultSnapshot.Failed.class);

        latch.countDown();
        absurd.awaitTaskResult(result.taskID(), queueName, 10);
    }

    @Test
    void stepsAreCheckpointed() throws Exception {
        absurd.registerTask("sae-steps", JsonValue.class, (params, ctx) -> {
            int a = ctx.step("step-a", Integer.class, () -> 10);
            int b = ctx.step("step-b", Integer.class, () -> 20);
            return a + b;
        });

        SpawnResult result = absurd.spawnAndExecute("sae-steps", null);

        var snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asInt()).isEqualTo(30);
    }

    // --- Failure & Retry ---

    @Test
    void handlerFailureFallsBackToNormalRetry() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask("sae-retry", JsonValue.class, (params, ctx) -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("first attempt fails");
            }
            return "success-on-retry";
        });

        SpawnResult result = absurd.spawnAndExecute("sae-retry", null,
                SpawnOptions.builder().maxAttempts(3).build());

        // Wait a bit for the local execution to fail
        Thread.sleep(500);

        // The task should not be completed yet (needs retry via normal worker)
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isNotInstanceOf(TaskResultSnapshot.Completed.class);

        // Normal worker picks up and retries
        absurd.workBatch("worker", 60, 1);

        snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("success-on-retry");
    }

    @Test
    void noLocalRetryOnFailure() throws Exception {
        AtomicInteger invocations = new AtomicInteger(0);

        absurd.registerTask("sae-no-local-retry", JsonValue.class, (params, ctx) -> {
            invocations.incrementAndGet();
            throw new RuntimeException("always fails");
        });

        absurd.spawnAndExecute("sae-no-local-retry", null,
                SpawnOptions.builder().maxAttempts(3).build());

        // Wait enough time for multiple retries if they happened locally
        Thread.sleep(1000);

        // Handler should have been invoked exactly once locally
        assertThat(invocations.get()).isEqualTo(1);
    }

    // --- Best-Effort / Race Condition ---

    @Test
    void silentlyBacksOffIfTaskAlreadyClaimed() throws Exception {
        // Strategy: We can't reliably win a race in a unit test, but we CAN
        // prove the mechanism works by calling claimSpecificTask on an already-claimed
        // run and verifying it returns empty (no crash, no exception).
        // Then we verify spawnAndExecute still works end-to-end when the task
        // completes via a normal worker instead.

        AtomicInteger localExecCount = new AtomicInteger(0);

        absurd.registerTask("sae-race", JsonValue.class, (params, ctx) -> {
            localExecCount.incrementAndGet();
            return "done";
        });

        // Step 1: Spawn a task normally and claim it immediately with workBatch
        SpawnResult result = absurd.spawn("sae-race", null);
        absurd.workBatch("worker-1", 60, 1);

        // Task is now completed by the worker
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);

        // Step 2: Attempt to claim the same run_id that was already processed.
        // This directly tests the claim_specific_task SQL function's back-off behavior.
        Optional<ClaimedTask> claimed = absurd.claimSpecificTask(
                queueName, result.runID(), 60, "late-claimer");

        // Should return empty — no crash, no exception
        assertThat(claimed).isEmpty();

        // Step 3: Verify spawnAndExecute returns normally even if the virtual thread
        // fails to claim (this is an end-to-end sanity check)
        SpawnResult result2 = absurd.spawnAndExecute("sae-race", null);
        assertThat(result2).isNotNull();
        assertThat(result2.created()).isTrue();

        // The second task completes (either locally or via the virtual thread)
        var snapshot2 = absurd.awaitTaskResult(result2.taskID(), queueName, 10);
        assertThat(snapshot2).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void worksWhenQueueHasOtherPendingTasks() throws Exception {
        absurd.registerTask("sae-queue-busy", JsonValue.class, (params, ctx) ->
                params.node().get("id").asText());

        // Spawn several tasks normally
        absurd.spawn("sae-queue-busy", Map.of("id", "task-1"));
        absurd.spawn("sae-queue-busy", Map.of("id", "task-2"));
        absurd.spawn("sae-queue-busy", Map.of("id", "task-3"));

        // Now spawnAndExecute a new one
        SpawnResult result = absurd.spawnAndExecute("sae-queue-busy", Map.of("id", "task-sae"));

        var snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("task-sae");
    }

    // --- Coexistence with Workers ---

    @Test
    void coexistsWithRunningWorker() throws Exception {
        absurd.registerTask("sae-coexist", JsonValue.class, (params, ctx) -> "coexist-ok");

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.1)
                .claimTimeout(60)
                .build());

        try {
            SpawnResult result = absurd.spawnAndExecute("sae-coexist", null);

            var snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
            assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        } finally {
            worker.close();
        }
    }

    @Test
    void noDoubleExecution() throws Exception {
        AtomicInteger execCount = new AtomicInteger(0);

        absurd.registerTask("sae-no-double", JsonValue.class, (params, ctx) -> {
            execCount.incrementAndGet();
            Thread.sleep(200); // Simulate some work
            return "done";
        });

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05)
                .claimTimeout(60)
                .build());

        try {
            SpawnResult result = absurd.spawnAndExecute("sae-no-double", null);

            absurd.awaitTaskResult(result.taskID(), queueName, 10);
            // Give extra time for any potential double execution
            Thread.sleep(500);

            assertThat(execCount.get()).isEqualTo(1);
        } finally {
            worker.close();
        }
    }

    // --- Suspend / Sleep ---

    @Test
    void taskThatSuspendsOnAwaitEvent() throws Exception {
        absurd.registerTask("sae-suspend-event", JsonValue.class, (params, ctx) -> {
            JsonValue event = ctx.awaitEvent("my-event");
            return event.node().get("data").asText();
        });

        SpawnResult result = absurd.spawnAndExecute("sae-suspend-event", null);

        // Wait for task to suspend
        Thread.sleep(500);
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);

        // Emit the event
        absurd.emitEvent("my-event", Map.of("data", "woken-up"));

        // Worker picks it up and completes
        absurd.workBatch("worker", 60, 1);
        snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("woken-up");
    }

    @Test
    void taskThatCallsSleepFor() throws Exception {
        absurd.registerTask("sae-sleep", JsonValue.class, (params, ctx) -> {
            ctx.sleepFor("sleep-step", 1); // Sleep for 1 second
            return "slept-ok";
        });

        SpawnResult result = absurd.spawnAndExecute("sae-sleep", null);

        // Wait for task to suspend
        Thread.sleep(500);
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);

        // Wait for sleep to expire, then run worker
        Thread.sleep(1500);
        absurd.workBatch("worker", 60, 1);

        snapshot = absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("slept-ok");
    }

    // --- Edge Cases ---

    @Test
    void unregisteredTaskNameThrows() {
        assertThatThrownBy(() -> absurd.spawnAndExecute("no-such-task-sae", null))
                .isInstanceOf(AbsurdException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void spawnOptionsAreRespected() throws Exception {
        AtomicReference<Object> capturedHeader = new AtomicReference<>();

        absurd.registerTask("sae-options", JsonValue.class, (params, ctx) -> {
            capturedHeader.set(ctx.headers().get("trace"));
            return "options-ok";
        });

        SpawnResult result = absurd.spawnAndExecute("sae-options", null,
                SpawnOptions.builder()
                        .maxAttempts(7)
                        .headers(Map.of("trace", "xyz-789"))
                        .build());

        absurd.awaitTaskResult(result.taskID(), queueName, 10);
        assertThat(capturedHeader.get()).isEqualTo("xyz-789");
    }

    @Test
    void idempotencyKeyDeduplication() throws Exception {
        AtomicInteger execCount = new AtomicInteger(0);

        absurd.registerTask("sae-idemp", JsonValue.class, (params, ctx) -> {
            execCount.incrementAndGet();
            return "idemp-ok";
        });

        SpawnResult first = absurd.spawnAndExecute("sae-idemp", null,
                SpawnOptions.builder().idempotencyKey("sae-key-1").build());

        SpawnResult second = absurd.spawnAndExecute("sae-idemp", null,
                SpawnOptions.builder().idempotencyKey("sae-key-1").build());

        assertThat(first.taskID()).isEqualTo(second.taskID());
        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();

        absurd.awaitTaskResult(first.taskID(), queueName, 10);

        // Wait for any background threads to settle
        Thread.sleep(500);

        // Handler should run at most once
        assertThat(execCount.get()).isEqualTo(1);
    }
}
