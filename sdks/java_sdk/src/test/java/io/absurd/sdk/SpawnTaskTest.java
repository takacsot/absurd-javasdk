package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpawnTaskTest {

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

        queueName = "spawn_test_q";
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
    void spawnSimple_createsAndExecutesTask() {
        absurd.registerTask("simple", JsonValue.class, (params, ctx) -> params.node().get("x").asInt());

        SpawnResult result = absurd.spawn("simple", Map.of("x", 7));
        assertThat(result.created()).isTrue();
        assertThat(result.taskID()).isNotNull();

        absurd.workBatch("worker", 60, 1);
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        var completed = (TaskResultSnapshot.Completed) snapshot;
        assertThat(completed.result().node().asInt()).isEqualTo(7);
    }

    @Test
    void spawnWithNullParams_succeeds() {
        absurd.registerTask("null-params", JsonValue.class, (params, ctx) -> "ok");

        SpawnResult result = absurd.spawn("null-params", null);
        assertThat(result.created()).isTrue();

        absurd.workBatch("worker", 60, 1);
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void spawnWithMaxAttempts_overridesDefault() {
        absurd.registerTask("max1", JsonValue.class, (params, ctx) -> {
            throw new RuntimeException("fail");
        });

        SpawnResult result = absurd.spawn("max1", null,
                SpawnOptions.builder().maxAttempts(1).build());

        absurd.workBatch("worker", 60, 1);
        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Failed.class);
    }

    @Test
    void spawnWithHeaders_accessibleInHandler() {
        AtomicReference<Object> captured = new AtomicReference<>();

        absurd.registerTask("headers-task", JsonValue.class, (params, ctx) -> {
            captured.set(ctx.headers().get("trace-id"));
            return "done";
        });

        absurd.spawn("headers-task", null,
                SpawnOptions.builder().headers(Map.of("trace-id", "abc-123")).build());

        absurd.workBatch("worker", 60, 1);
        assertThat(captured.get()).isEqualTo("abc-123");
    }

    @Test
    void spawnWithIdempotencyKey_deduplicates() {
        absurd.registerTask("idemp", JsonValue.class, (params, ctx) -> "ok");

        SpawnResult first = absurd.spawn("idemp", null,
                SpawnOptions.builder().idempotencyKey("key-1").build());
        SpawnResult second = absurd.spawn("idemp", null,
                SpawnOptions.builder().idempotencyKey("key-1").build());

        assertThat(first.taskID()).isEqualTo(second.taskID());
        assertThat(first.created()).isTrue();
        assertThat(second.created()).isFalse();
    }

    @Test
    void spawnWithIdempotencyKey_differentKeysCreateSeparateTasks() {
        absurd.registerTask("idemp2", JsonValue.class, (params, ctx) -> "ok");

        SpawnResult first = absurd.spawn("idemp2", null,
                SpawnOptions.builder().idempotencyKey("key-a").build());
        SpawnResult second = absurd.spawn("idemp2", null,
                SpawnOptions.builder().idempotencyKey("key-b").build());

        assertThat(first.taskID()).isNotEqualTo(second.taskID());
        assertThat(first.created()).isTrue();
        assertThat(second.created()).isTrue();
    }

    @Test
    void spawnWithCancellation_overridesRegistration() throws Exception {
        absurd.registerTask(TaskRegistration.builder("cancel-override")
                .defaultCancellation(CancellationPolicy.of(60, null))
                .handler(JsonValue.class, (params, ctx) -> {
                    ctx.awaitEvent("never");
                    return null;
                })
                .build());

        SpawnResult result = absurd.spawn("cancel-override", null,
                SpawnOptions.builder().cancellation(CancellationPolicy.of(1, null)).build());

        absurd.workBatch("worker", 60, 1);
        Thread.sleep(1500);
        absurd.workBatch("worker", 60, 1);

        var snapshot = absurd.fetchTaskResult(result.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Cancelled.class);
    }

    @Test
    void spawnUnregisteredWithQueue_succeeds() {
        SpawnResult result = absurd.spawn("unregistered-task", Map.of("a", 1),
                SpawnOptions.builder().queue(queueName).build());
        assertThat(result.created()).isTrue();
    }

    @Test
    void spawnUnregisteredWithoutQueue_throws() {
        assertThatThrownBy(() -> absurd.spawn("no-such-task", null))
                .isInstanceOf(AbsurdException.class)
                .hasMessageContaining("not registered");
    }

    @Test
    void spawnWithQueueMismatch_throws() {
        absurd.registerTask(TaskRegistration.builder("queue-bound")
                .queue(queueName)
                .handler(JsonValue.class, (params, ctx) -> null)
                .build());

        assertThatThrownBy(() -> absurd.spawn("queue-bound", null,
                SpawnOptions.builder().queue("other_queue").build()))
                .isInstanceOf(AbsurdException.class)
                .hasMessageContaining("queue");
    }

    @Test
    void spawnWithHandle_commitsWithTransaction() {
        absurd.registerTask("handle-commit", JsonValue.class, (params, ctx) -> "ok");

        Jdbi jdbi = Jdbi.create(dataSource);
        SpawnResult[] result = new SpawnResult[1];

        jdbi.useTransaction(handle -> {
            result[0] = absurd.spawn(handle, "handle-commit", Map.of("k", "v"));
        });

        var snapshot = absurd.fetchTaskResult(result[0].taskID());
        assertThat(snapshot).isNotNull();
    }

    @Test
    void spawnWithHandle_rollsBackWithTransaction() {
        absurd.registerTask("handle-rollback", JsonValue.class, (params, ctx) -> "ok");

        Jdbi jdbi = Jdbi.create(dataSource);
        AtomicReference<String> taskId = new AtomicReference<>();

        try {
            jdbi.useTransaction(handle -> {
                taskId.set(absurd.spawn(handle, "handle-rollback", null).taskID());
                throw new RuntimeException("rollback");
            });
        } catch (RuntimeException ignored) {}

        var snapshot = absurd.fetchTaskResult(taskId.get());
        assertThat(snapshot).isNull();
    }

    @Test
    void spawnWithConnection_commitsWithTransaction() throws Exception {
        absurd.registerTask("conn-commit", JsonValue.class, (params, ctx) -> "ok");

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            SpawnResult result = absurd.spawn(conn, "conn-commit", Map.of("x", 1));
            conn.commit();

            var snapshot = absurd.fetchTaskResult(result.taskID());
            assertThat(snapshot).isNotNull();
        }
    }

    @Test
    void spawnWithConnection_rollsBackWithTransaction() throws Exception {
        absurd.registerTask("conn-rollback", JsonValue.class, (params, ctx) -> "ok");

        String taskId;
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            taskId = absurd.spawn(conn, "conn-rollback", null).taskID();
            conn.rollback();
        }

        var snapshot = absurd.fetchTaskResult(taskId);
        assertThat(snapshot).isNull();
    }
}
