package io.absurd.sdk;

import org.junit.jupiter.api.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RegisterTaskTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("reg_test_q");
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
    void registerWithShorthand_executesTask() {
        absurd.registerTask("shorthand-task", JsonValue.class, (params, ctx) -> "done");

        SpawnResult spawned = absurd.spawn("shorthand-task", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void registerWithBuilder_defaultOptions() {
        absurd.registerTask(TaskRegistration.builder("builder-default")
                .handler(JsonValue.class, (params, ctx) -> "ok")
                .build());

        SpawnResult spawned = absurd.spawn("builder-default", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void registerWithBuilder_customQueue() {
        String customQueue = "custom_q";
        absurd.createQueue(customQueue);

        absurd.registerTask(TaskRegistration.builder("queue-task")
                .queue(customQueue)
                .handler(JsonValue.class, (params, ctx) -> "routed")
                .build());

        SpawnResult spawned = absurd.spawn("queue-task", null,
                SpawnOptions.builder().queue(customQueue).build());

        // Work from the custom queue
        Absurd customAbsurd = Absurd.create(dataSource, customQueue);
        customAbsurd.registerTask(TaskRegistration.builder("queue-task")
                .handler(JsonValue.class, (params, ctx) -> "routed")
                .build());
        customAbsurd.workBatch("worker", 60, 1);

        var snapshot = customAbsurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        customAbsurd.close();
    }

    @Test
    void registerWithBuilder_customMaxAttempts() {
        absurd.registerTask(TaskRegistration.builder("max-attempts-task")
                .defaultMaxAttempts(2)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("fail");
                })
                .build());

        SpawnResult spawned = absurd.spawn("max-attempts-task", null);
        absurd.workBatch("worker", 60, 1);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
    }

    @Test
    void registerWithBuilder_cancellationPolicy() throws Exception {
        absurd.registerTask(TaskRegistration.builder("cancel-policy-task")
                .defaultCancellation(CancellationPolicy.of(1, null))
                .handler(JsonValue.class, (params, ctx) -> {
                    ctx.awaitEvent("never-arrives");
                    return null;
                })
                .build());

        SpawnResult spawned = absurd.spawn("cancel-policy-task", null);
        absurd.workBatch("worker", 60, 1);

        // Wait for maxDuration (1s) to elapse
        Thread.sleep(1500);

        // Attempt to work again — the DB should cancel the task on claim
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Cancelled.class);
    }

    @Test
    void registerWithTypedParams() {
        record MyParams(String name, int value) {}

        absurd.registerTask(TaskRegistration.builder("typed-params")
                .handler(MyParams.class, (params, ctx) -> params.name() + ":" + params.value())
                .build());

        SpawnResult spawned = absurd.spawn("typed-params", java.util.Map.of("name", "hello", "value", 42));
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("hello:42");
    }

    @Test
    void registerWithJsonValueParams() {
        absurd.registerTask(TaskRegistration.builder("json-params")
                .handler(JsonValue.class, (params, ctx) -> params.node().get("key").asText())
                .build());

        SpawnResult spawned = absurd.spawn("json-params", java.util.Map.of("key", "value"));
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("value");
    }

    @Test
    void registerDuplicateName_overridesPrevious() {
        absurd.registerTask(TaskRegistration.builder("dup-task")
                .handler(JsonValue.class, (params, ctx) -> "first")
                .build());

        absurd.registerTask(TaskRegistration.builder("dup-task")
                .handler(JsonValue.class, (params, ctx) -> "second")
                .build());

        SpawnResult spawned = absurd.spawn("dup-task", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asText()).isEqualTo("second");
    }

    @Test
    void registerWithNullQueue_usesClientDefault() {
        absurd.registerTask(TaskRegistration.builder("null-queue-task")
                .handler(JsonValue.class, (params, ctx) -> "default-queue")
                .build());

        SpawnResult spawned = absurd.spawn("null-queue-task", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void builderWithoutHandler_throwsOnBuild() {
        assertThatThrownBy(() -> TaskRegistration.builder("no-handler").build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("handler");
    }
}
