package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TaskResultTest {

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

        Path schemaPath = Path.of("../../sql/absurd.sql");
        String schema = Files.readString(schemaPath);
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(schema);
        }

        queueName = "result_test_q";
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
            } catch (Exception ignored) {}
        });
    }

    @Test
    void fetchTaskResult_returnsCompleted() {
        absurd.registerTask("fetch-done", JsonValue.class, (params, ctx) -> 42);

        SpawnResult spawned = absurd.spawn("fetch-done", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        assertThat(((TaskResultSnapshot.Completed) snapshot).result().node().asInt()).isEqualTo(42);
    }

    @Test
    void fetchTaskResult_returnsPendingBeforeExecution() {
        absurd.registerTask("fetch-pending", JsonValue.class, (params, ctx) -> "ok");

        SpawnResult spawned = absurd.spawn("fetch-pending", null);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Pending.class);
    }

    @Test
    void fetchTaskResult_returnsSleepingForSuspendedTask() {
        absurd.registerTask("fetch-sleeping", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("never");
            return null;
        });

        SpawnResult spawned = absurd.spawn("fetch-sleeping", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Sleeping.class);
    }

    @Test
    void fetchTaskResult_returnsFailed() {
        absurd.registerTask(TaskRegistration.builder("fetch-failed")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("boom");
                })
                .build());

        SpawnResult spawned = absurd.spawn("fetch-failed", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
        assertThat(((TaskResultSnapshot.Failed) snapshot).failure()).isNotNull();
    }

    @Test
    void fetchTaskResult_returnsCancelled() {
        absurd.registerTask("fetch-cancelled", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("never");
            return null;
        });

        SpawnResult spawned = absurd.spawn("fetch-cancelled", null);
        absurd.workBatch("worker", 60, 1);
        absurd.cancelTask(spawned.taskID());

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Cancelled.class);
    }

    @Test
    void fetchTaskResult_returnsNullForNonexistentTask() {
        var snapshot = absurd.fetchTaskResult(UUID.randomUUID().toString());
        assertThat(snapshot).isNull();
    }

    @Test
    void fetchTaskResult_withExplicitQueue() {
        absurd.registerTask("fetch-explicit-q", JsonValue.class, (params, ctx) -> "yes");

        SpawnResult spawned = absurd.spawn("fetch-explicit-q", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID(), queueName);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void awaitTaskResult_returnsImmediatelyIfAlreadyTerminal() {
        absurd.registerTask("await-immediate", JsonValue.class, (params, ctx) -> "fast");

        SpawnResult spawned = absurd.spawn("await-immediate", null);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.awaitTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void awaitTaskResult_blocksUntilCompletion() throws Exception {
        absurd.registerTask("await-block", JsonValue.class, (params, ctx) -> "delayed");

        SpawnResult spawned = absurd.spawn("await-block", null);

        // Work on another thread after a short delay
        CompletableFuture.runAsync(() -> {
            try { Thread.sleep(200); } catch (InterruptedException ignored) {}
            absurd.workBatch("worker", 60, 1);
        });

        var snapshot = absurd.awaitTaskResult(spawned.taskID(), null, 5);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void awaitTaskResult_timeoutThrowsWhenNotTerminal() {
        absurd.registerTask("await-timeout", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("never");
            return null;
        });

        SpawnResult spawned = absurd.spawn("await-timeout", null);
        absurd.workBatch("worker", 60, 1);

        assertThatThrownBy(() -> absurd.awaitTaskResult(spawned.taskID(), null, 1))
                .isInstanceOf(TimeoutException.class);
    }

    @Test
    void awaitTaskResult_throwsForNonexistentTask() {
        assertThatThrownBy(() -> absurd.awaitTaskResult(UUID.randomUUID().toString()))
                .isInstanceOf(AbsurdException.class)
                .hasMessageContaining("not found");
    }

    @Test
    void awaitTaskResult_negativeTimeoutThrows() {
        absurd.registerTask("await-neg", JsonValue.class, (params, ctx) -> "ok");
        SpawnResult spawned = absurd.spawn("await-neg", null);

        assertThatThrownBy(() -> absurd.awaitTaskResult(spawned.taskID(), null, -1))
                .isInstanceOf(AbsurdException.class);
    }

    @Test
    void awaitTaskResult_nullTimeoutWaitsIndefinitely() throws Exception {
        absurd.registerTask("await-null-timeout", JsonValue.class, (params, ctx) -> "eventual");

        SpawnResult spawned = absurd.spawn("await-null-timeout", null);

        CompletableFuture.runAsync(() -> {
            try { Thread.sleep(300); } catch (InterruptedException ignored) {}
            absurd.workBatch("worker", 60, 1);
        });

        // null timeout — waits until terminal (task completes after ~300ms)
        var snapshot = absurd.awaitTaskResult(spawned.taskID(), null, null);
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }
}
