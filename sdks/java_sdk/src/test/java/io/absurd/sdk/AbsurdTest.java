package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class AbsurdTest {

    static EmbeddedPostgres pg;
    static HikariDataSource dataSource;
    static Absurd absurd;
    static String queueName;

    @BeforeAll
    static void setup() throws Exception {
        pg = EmbeddedPostgres.start();

        HikariConfig config = new HikariConfig();
        config.setDataSource(pg.getPostgresDatabase());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);

        // Load schema (use raw JDBC to handle $$ dollar-quoting)
        Path schemaPath = Path.of("../../sql/absurd.sql");
        String schema = Files.readString(schemaPath);
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(schema);
        }

        queueName = "test_queue_" + System.currentTimeMillis();
        absurd = Absurd.create(dataSource, queueName);
        absurd.createQueue(queueName);
    }

    @AfterAll
    static void teardown() throws Exception {
        if (absurd != null) absurd.close();
        if (dataSource != null) dataSource.close();
        if (pg != null) pg.close();
    }

    @AfterEach
    void cleanupTasks() {
        Jdbi.create(dataSource).useHandle(h -> {
            try {
                h.execute("TRUNCATE absurd.t_" + queueName + ", absurd.r_" + queueName +
                          ", absurd.c_" + queueName + ", absurd.e_" + queueName +
                          ", absurd.w_" + queueName);
            } catch (Exception ignored) {
            }
        });
    }

    @Test
    @Order(1)
    void createListAndDropQueue() {
        String q = "java_test_q_" + System.currentTimeMillis();
        absurd.createQueue(q);

        var queues = absurd.listQueues();
        assertThat(queues).contains(q);

        absurd.dropQueue(q);

        queues = absurd.listQueues();
        assertThat(queues).doesNotContain(q);
    }

    @Test
    @Order(2)
    void spawnAndCompleteTask() {
        absurd.registerTask(TaskRegistration.builder("simple-task")
                .handler(JsonValue.class, (params, ctx) -> {
                    var result = ctx.step("compute", () -> 42);
                    return result;
                })
                .build());

        SpawnResult spawned = absurd.spawn("simple-task", null);
        assertThat(spawned.taskID()).isNotNull();
        assertThat(spawned.created()).isTrue();

        absurd.workBatch("test-worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    @Order(3)
    void stepResultIsCachedOnRetry() {
        AtomicInteger executionCount = new AtomicInteger(0);
        AtomicInteger attemptCount = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("cached-step")
                .defaultMaxAttempts(2)
                .handler(JsonValue.class, (params, ctx) -> {
                    int attempt = attemptCount.incrementAndGet();

                    var cached = ctx.step("generate", () -> {
                        executionCount.incrementAndGet();
                        return "computed-value";
                    });

                    if (attempt == 1) {
                        throw new RuntimeException("Intentional failure");
                    }
                    return cached;
                })
                .build());

        SpawnResult spawned = absurd.spawn("cached-step", null);

        absurd.workBatch("worker", 60, 1);
        assertThat(executionCount.get()).isEqualTo(1);

        absurd.workBatch("worker", 60, 1);
        assertThat(executionCount.get()).isEqualTo(1);
        assertThat(attemptCount.get()).isEqualTo(2);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    @Order(4)
    void taskFailsAfterRetriesExhausted() {
        absurd.registerTask(TaskRegistration.builder("always-fails")
                .defaultMaxAttempts(2)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("Always fails");
                })
                .build());

        SpawnResult spawned = absurd.spawn("always-fails", null);

        absurd.workBatch("worker", 60, 1);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
    }

    @Test
    @Order(5)
    void eventSystemWakesSleepingTask() {
        String eventName = "test-event-" + System.currentTimeMillis();


        absurd.registerTask(TaskRegistration.builder("event-waiter")
                .handler(JsonValue.class, (params, ctx) -> {
                    JsonValue payload = ctx.awaitEvent(eventName);
                    return payload;
                })
                .build());

        SpawnResult spawned = absurd.spawn("event-waiter", null);

        absurd.workBatch("worker", 60, 1);
        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);

        absurd.emitEvent(eventName, java.util.Map.of("data", "hello"));

        absurd.workBatch("worker", 60, 1);
        snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().get("data").asText()).isEqualTo("hello");
    }

    @Test
    @Order(6)
    void rejectsUnregisteredTaskWithoutQueue() {
        assertThatThrownBy(() -> absurd.spawn("nonexistent", null))
                .isInstanceOf(AbsurdException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    @Order(7)
    void cancelTask() {
        absurd.registerTask(TaskRegistration.builder("cancel-me")
                .handler(JsonValue.class, (params, ctx) -> {
                    ctx.awaitEvent("never-arrives");
                    return null;
                })
                .build());

        SpawnResult spawned = absurd.spawn("cancel-me", null);
        absurd.workBatch("worker", 60, 1);

        absurd.cancelTask(spawned.taskID());

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Cancelled.class);
    }

    @Test
    @Order(8)
    void workerProcessesTasks() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        AtomicReference<Exception> error = new AtomicReference<>();

        absurd.registerTask(TaskRegistration.builder("worker-task")
                .handler(JsonValue.class, (params, ctx) -> {
                    latch.countDown();
                    return "done";
                })
                .build());

        for (int i = 0; i < 3; i++) {
            absurd.spawn("worker-task", null);
        }

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(2)
                .pollIntervalSeconds(0.05)
                .onError(error::set)
                .build());

        boolean completed = latch.await(10, TimeUnit.SECONDS);
        worker.close();

        assertThat(completed).isTrue();
        assertThat(error.get()).isNull();
    }

    @Test
    @Order(9)
    void multipleStepsExecuteInOrder() {
        absurd.registerTask(TaskRegistration.builder("multi-step")
                .handler(JsonValue.class, (params, ctx) -> {
                    var s1 = ctx.step("step1", () -> "first");
                    var s2 = ctx.step("step2", () -> "second");
                    var s3 = ctx.step("step3", () -> "third");
                    return java.util.Map.of("s1", s1, "s2", s2, "s3", s3);
                })
                .build());

        SpawnResult spawned = absurd.spawn("multi-step", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().get("s1").asText()).isEqualTo("first");
        assertThat(completed.result().node().get("s2").asText()).isEqualTo("second");
        assertThat(completed.result().node().get("s3").asText()).isEqualTo("third");
    }

    @Test
    @Order(10)
    void unknownTaskIsDeferred() {
        SpawnResult spawned = absurd.spawn("ghost-task", null,
                SpawnOptions.builder().queue(queueName).maxAttempts(1).build());

        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);
    }

    @Test
    @Order(11)
    void spawnWithExternalHandleCommits() {
        absurd.registerTask(TaskRegistration.builder("outbox-task")
                .handler(JsonValue.class, (params, ctx) -> params)
                .build());

        Jdbi jdbi = Jdbi.create(dataSource);
        SpawnResult[] result = new SpawnResult[1];

        // Spawn inside an explicit transaction that commits
        jdbi.useTransaction(handle -> {
            result[0] = absurd.spawn(handle, "outbox-task", java.util.Map.of("key", "committed"));
        });

        assertThat(result[0]).isNotNull();
        assertThat(result[0].created()).isTrue();

        // Task should be visible and processable
        var snapshot = absurd.fetchTaskResult(result[0].taskID());
        assertThat(snapshot).isNotNull();
        assertThat(snapshot.state()).isEqualTo("pending");

        absurd.workBatch("worker", 60, 1);
        snapshot = absurd.fetchTaskResult(result[0].taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    @Order(12)
    void spawnWithExternalHandleRollsBack() {
        absurd.registerTask(TaskRegistration.builder("outbox-rollback")
                .handler(JsonValue.class, (params, ctx) -> params)
                .build());

        Jdbi jdbi = Jdbi.create(dataSource);
        AtomicReference<String> spawnedTaskId = new AtomicReference<>();

        // Spawn inside a transaction that rolls back
        try {
            jdbi.useTransaction(handle -> {
                SpawnResult r = absurd.spawn(handle, "outbox-rollback", java.util.Map.of("key", "rolled-back"));
                spawnedTaskId.set(r.taskID());
                throw new RuntimeException("Force rollback");
            });
        } catch (RuntimeException ignored) {}

        // Task should NOT exist after rollback
        var snapshot = absurd.fetchTaskResult(spawnedTaskId.get());
        assertThat(snapshot).isNull();
    }
}
