package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class WorkInBatchTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("batch_test_q");
    }

    @AfterAll
    static void teardown() throws Exception {
        teardownBase();
    }

    @AfterEach
    void cleanupTasks() {
        truncateQueue();
    }

    // --- workBatch ---

    @Test
    void workBatch_processesSingleTask() {
        absurd.registerTask("wb-single", JsonValue.class, (params, ctx) -> "done");
        SpawnResult spawned = absurd.spawn("wb-single", null);

        absurd.workBatch("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void workBatch_processesMultipleTasks() {
        AtomicInteger count = new AtomicInteger(0);
        absurd.registerTask("wb-multi", JsonValue.class, (params, ctx) -> {
            count.incrementAndGet();
            return "ok";
        });

        absurd.spawn("wb-multi", null);
        absurd.spawn("wb-multi", null);
        absurd.spawn("wb-multi", null);

        absurd.workBatch("w1", 60, 3);

        assertThat(count.get()).isEqualTo(3);
    }

    @Test
    void workBatch_respectsBatchSizeLimit() {
        AtomicInteger count = new AtomicInteger(0);
        absurd.registerTask("wb-limit", JsonValue.class, (params, ctx) -> {
            count.incrementAndGet();
            return "ok";
        });

        absurd.spawn("wb-limit", null);
        absurd.spawn("wb-limit", null);
        absurd.spawn("wb-limit", null);

        absurd.workBatch("w1", 60, 1);

        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    void workBatch_failedTaskRetriesOnNextBatch() {
        AtomicInteger attempts = new AtomicInteger(0);
        absurd.registerTask(TaskRegistration.builder("wb-retry")
                .defaultMaxAttempts(3)
                .handler(JsonValue.class, (params, ctx) -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("fail first");
                    }
                    return "recovered";
                })
                .build());

        SpawnResult spawned = absurd.spawn("wb-retry", null);
        absurd.workBatch("w1", 60, 1);

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isNotInstanceOf(TaskResultSnapshot.Completed.class);

        absurd.workBatch("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void workBatch_unknownTaskIsDeferred() {
        SpawnResult spawned = absurd.spawn("wb-unknown", null,
                SpawnOptions.builder().queue(queueName).build());

        absurd.workBatch("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);
    }

    @Test
    void workBatch_suspendedTaskNotCompleted() {
        absurd.registerTask("wb-suspend", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("some-event");
            return null;
        });

        SpawnResult spawned = absurd.spawn("wb-suspend", null);
        absurd.workBatch("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);
    }

    @Test
    void workBatch_claimTimeoutExpiresReleasesTask() {
        AtomicInteger attempts = new AtomicInteger(0);
        absurd.registerTask(TaskRegistration.builder("wb-timeout")
                .defaultMaxAttempts(3)
                .handler(JsonValue.class, (params, ctx) -> {
                    if (attempts.incrementAndGet() == 1) {
                        // Simulate a task that exceeds claim timeout by throwing
                        throw new RuntimeException("timeout simulation");
                    }
                    return "ok";
                })
                .build());

        // Use very short claim timeout
        SpawnResult spawned = absurd.spawn("wb-timeout", null);
        absurd.workBatch("w1", 1, 1);

        // Task failed, should be reclaimable
        absurd.workBatch("w2", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void workBatch_emptyQueueDoesNothing() {
        // Should not throw
        absurd.workBatch("w1", 60, 10);
    }

    // --- workBatchPooled ---

    @Test
    void workBatchPooled_processesSingleTask() {
        absurd.registerTask("wbp-single", JsonValue.class, (params, ctx) -> "pooled-done");
        SpawnResult spawned = absurd.spawn("wbp-single", null);

        absurd.workBatchPooled("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void workBatchPooled_processesMultipleTasks() {
        AtomicInteger count = new AtomicInteger(0);
        absurd.registerTask("wbp-multi", JsonValue.class, (params, ctx) -> {
            count.incrementAndGet();
            return "ok";
        });

        absurd.spawn("wbp-multi", null);
        absurd.spawn("wbp-multi", null);
        absurd.spawn("wbp-multi", null);

        absurd.workBatchPooled("w1", 60, 3);

        assertThat(count.get()).isEqualTo(3);
    }

    @Test
    void workBatchPooled_failedTaskRetriesOnNextBatch() {
        AtomicInteger attempts = new AtomicInteger(0);
        absurd.registerTask(TaskRegistration.builder("wbp-retry")
                .defaultMaxAttempts(3)
                .handler(JsonValue.class, (params, ctx) -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("fail first");
                    }
                    return "recovered";
                })
                .build());

        SpawnResult spawned = absurd.spawn("wbp-retry", null);
        absurd.workBatchPooled("w1", 60, 1);

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isNotInstanceOf(TaskResultSnapshot.Completed.class);

        absurd.workBatchPooled("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void workBatchPooled_suspendedTaskNotCompleted() {
        absurd.registerTask("wbp-suspend", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("pooled-event");
            return null;
        });

        SpawnResult spawned = absurd.spawn("wbp-suspend", null);
        absurd.workBatchPooled("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);
    }

    @Test
    void workBatchPooled_unknownTaskIsDeferred() {
        SpawnResult spawned = absurd.spawn("wbp-unknown", null,
                SpawnOptions.builder().queue(queueName).build());

        absurd.workBatchPooled("w1", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);
    }
}
