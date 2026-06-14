package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("retry_test_q");
    }

    @AfterAll
    static void teardown() throws Exception {
        teardownBase();
    }

    @AfterEach
    void cleanupTasks() {
        truncateQueue();
    }

    @Test
    void retryFailedTask_createsNewRun() {
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("retry-fail")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("first attempt fails");
                    }
                    return "success";
                })
                .build());

        SpawnResult spawned = absurd.spawn("retry-fail", null);
        absurd.workBatch("worker", 60, 1);

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isInstanceOf(TaskResultSnapshot.Failed.class);

        SpawnResult retried = absurd.retryTask(spawned.taskID());
        assertThat(retried.taskID()).isEqualTo(spawned.taskID());

        absurd.workBatch("worker", 60, 1);
        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void retryCancelledTask_throws() {
        absurd.registerTask("retry-cancel", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("never");
            return null;
        });

        SpawnResult spawned = absurd.spawn("retry-cancel", null);
        absurd.workBatch("worker", 60, 1);
        absurd.cancelTask(spawned.taskID());

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isInstanceOf(TaskResultSnapshot.Cancelled.class);

        assertThatThrownBy(() -> absurd.retryTask(spawned.taskID()))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("not currently failed");
    }

    @Test
    void retryWithNewMaxAttempts() {
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("retry-max")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    if (attempts.incrementAndGet() <= 2) {
                        throw new RuntimeException("fail");
                    }
                    return "ok";
                })
                .build());

        SpawnResult spawned = absurd.spawn("retry-max", null);
        absurd.workBatch("worker", 60, 1);
        assertThat(absurd.fetchTaskResult(spawned.taskID())).isInstanceOf(TaskResultSnapshot.Failed.class);

        // Retry with 3 max attempts — enough for attempt #3 to succeed
        absurd.retryTask(spawned.taskID(), null, 3, false);
        absurd.workBatch("worker", 60, 1);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void retryWithSpawnNew_createsNewTaskId() {
        absurd.registerTask(TaskRegistration.builder("retry-spawn-new")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("fail");
                })
                .build());

        SpawnResult spawned = absurd.spawn("retry-spawn-new", null);
        absurd.workBatch("worker", 60, 1);

        SpawnResult retried = absurd.retryTask(spawned.taskID(), null, null, true);
        assertThat(retried.taskID()).isNotEqualTo(spawned.taskID());
        assertThat(retried.created()).isTrue();
    }

    @Test
    void retryWithSpawnNew_false_keepsSameTaskId() {
        absurd.registerTask(TaskRegistration.builder("retry-same-id")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("fail");
                })
                .build());

        SpawnResult spawned = absurd.spawn("retry-same-id", null);
        absurd.workBatch("worker", 60, 1);

        SpawnResult retried = absurd.retryTask(spawned.taskID(), null, null, false);
        assertThat(retried.taskID()).isEqualTo(spawned.taskID());
        assertThat(retried.created()).isFalse();
    }

    @Test
    void retryWithExplicitQueue() {
        absurd.registerTask(TaskRegistration.builder("retry-explicit-q")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("fail");
                })
                .build());

        SpawnResult spawned = absurd.spawn("retry-explicit-q", null);
        absurd.workBatch("worker", 60, 1);

        SpawnResult retried = absurd.retryTask(spawned.taskID(), queueName, null, false);
        assertThat(retried.taskID()).isEqualTo(spawned.taskID());
    }

    @Test
    void retryNonTerminalTask_throws() {
        absurd.registerTask("retry-pending", JsonValue.class, (params, ctx) -> "ok");

        SpawnResult spawned = absurd.spawn("retry-pending", null);

        assertThatThrownBy(() -> absurd.retryTask(spawned.taskID()))
                .isInstanceOf(Exception.class);
    }

    @Test
    void retryNonexistentTask_throws() {
        assertThatThrownBy(() -> absurd.retryTask(UUID.randomUUID().toString()))
                .isInstanceOf(Exception.class);
    }
}
