package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChildTaskTest extends AbstractAbsurdTest {

    private static Absurd childAbsurd;
    private static final String CHILD_QUEUE = "child_q";

    @BeforeAll
    static void setup() throws Exception {
        setupBase("parent_q");
        childAbsurd = Absurd.create(dataSource, CHILD_QUEUE);
        childAbsurd.createQueue(CHILD_QUEUE);
    }

    @AfterAll
    static void teardown() throws Exception {
        if (childAbsurd != null) childAbsurd.close();
        teardownBase();
    }

    @AfterEach
    void cleanup() {
        truncateQueue();
        // Also truncate child queue
        org.jdbi.v3.core.Jdbi.create(dataSource).useHandle(h -> {
            try {
                h.execute("TRUNCATE absurd.t_" + CHILD_QUEUE + ", absurd.r_" + CHILD_QUEUE +
                        ", absurd.c_" + CHILD_QUEUE + ", absurd.e_" + CHILD_QUEUE +
                        ", absurd.w_" + CHILD_QUEUE);
            } catch (Exception ignored) {}
        });
    }

    @Test
    void awaitTaskResult_completedChild() {
        childAbsurd.registerTask("child-complete", JsonValue.class, (params, ctx) -> {
            return Map.of("answer", 42);
        });

        absurd.registerTask("parent-await", JsonValue.class, (params, ctx) -> {
            SpawnResult child = ctx.step("spawn", SpawnResult.class, () ->
                childAbsurd.spawn("child-complete", null));
            // Run child before parent polls
            childAbsurd.workBatch("cw", 60, 1);
            TaskResultSnapshot result = ctx.awaitTaskResult(child.taskID(), CHILD_QUEUE, 5);
            assertThat(result).isInstanceOf(TaskResultSnapshot.Completed.class);
            return ((TaskResultSnapshot.Completed) result).result().node();
        });

        SpawnResult parent = absurd.spawn("parent-await", null);
        absurd.workBatch("pw", 60, 1);

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(parent.taskID());
        assertThat(snapshot.result().node().get("answer").asInt()).isEqualTo(42);
    }

    @Test
    void awaitTaskResult_sameQueueThrows() {
        absurd.registerTask(TaskRegistration.builder("parent-same-q")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    ctx.awaitTaskResult("fake-id", queueName, 5);
                    return null;
                })
                .build());

        SpawnResult parent = absurd.spawn("parent-same-q", null);
        absurd.workBatch("pw", 60, 1);

        var snapshot = absurd.fetchTaskResult(parent.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
    }

    @Test
    void awaitTaskResult_nullQueueThrows() {
        absurd.registerTask(TaskRegistration.builder("parent-null-q")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    ctx.awaitTaskResult("fake-id", null, 5);
                    return null;
                })
                .build());

        SpawnResult parent = absurd.spawn("parent-null-q", null);
        absurd.workBatch("pw", 60, 1);

        var snapshot = absurd.fetchTaskResult(parent.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
    }

    @Test
    void awaitTaskResult_timeout() {
        childAbsurd.registerTask("child-slow", JsonValue.class, (params, ctx) -> {
            ctx.sleepFor("long-sleep", 60);
            return "done";
        });

        absurd.registerTask(TaskRegistration.builder("parent-timeout")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    SpawnResult child = ctx.step("spawn", SpawnResult.class, () ->
                        childAbsurd.spawn("child-slow", null));
                    childAbsurd.workBatch("cw", 60, 1); // child suspends sleeping
                    ctx.awaitTaskResult(child.taskID(), CHILD_QUEUE, 1);
                    return "unreachable";
                })
                .build());

        SpawnResult parent = absurd.spawn("parent-timeout", null);
        absurd.workBatch("pw", 60, 1);

        var snapshot = absurd.fetchTaskResult(parent.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
    }

    @Test
    void awaitTaskResult_failedChild() {
        childAbsurd.registerTask(TaskRegistration.builder("child-fail")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("child exploded");
                })
                .build());

        absurd.registerTask("parent-fail-child", JsonValue.class, (params, ctx) -> {
            SpawnResult child = ctx.step("spawn", SpawnResult.class, () ->
                childAbsurd.spawn("child-fail", null));
            childAbsurd.workBatch("cw", 60, 1); // child fails
            TaskResultSnapshot result = ctx.awaitTaskResult(child.taskID(), CHILD_QUEUE, 5);
            return Map.of("childState", result.state());
        });

        SpawnResult parent = absurd.spawn("parent-fail-child", null);
        absurd.workBatch("pw", 60, 1);

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(parent.taskID());
        assertThat(snapshot.result().node().get("childState").asText()).isEqualTo("failed");
    }

    @Test
    void awaitTaskResult_checkpointedOnRetry() {
        AtomicReference<String> childId = new AtomicReference<>();

        childAbsurd.registerTask("child-ok", JsonValue.class, (params, ctx) -> "child-done");

        AtomicReference<Integer> attempts = new AtomicReference<>(0);
        absurd.registerTask(TaskRegistration.builder("parent-retry-await")
                .defaultMaxAttempts(3)
                .handler(JsonValue.class, (params, ctx) -> {
                    SpawnResult child = ctx.step("spawn", SpawnResult.class, () ->
                        childAbsurd.spawn("child-ok", null));
                    childId.set(child.taskID());
                    childAbsurd.workBatch("cw", 60, 1);
                    TaskResultSnapshot result = ctx.awaitTaskResult(child.taskID(), CHILD_QUEUE, 5);
                    if (attempts.getAndSet(attempts.get() + 1) == 0) {
                        throw new RuntimeException("fail after await");
                    }
                    return Map.of("state", result.state());
                })
                .build());

        SpawnResult parent = absurd.spawn("parent-retry-await", null);
        absurd.workBatch("pw", 60, 1); // first attempt: awaits child, then fails
        absurd.workBatch("pw", 60, 1); // second attempt: checkpoint cached, succeeds

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(parent.taskID());
        assertThat(snapshot.result().node().get("state").asText()).isEqualTo("completed");
    }
}
