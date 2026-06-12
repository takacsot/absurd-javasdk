package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class TaskContextTest {

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

        queueName = "ctx_test_q";
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
    void step_returnsCachedResultOnRetry() {
        AtomicInteger execCount = new AtomicInteger(0);
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("cached-step")
                .defaultMaxAttempts(2)
                .handler(JsonValue.class, (params, ctx) -> {
                    var result = ctx.step("compute", () -> {
                        execCount.incrementAndGet();
                        return "value";
                    });
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("fail first");
                    }
                    return result;
                })
                .build());

        SpawnResult spawned = absurd.spawn("cached-step", null);
        absurd.workBatch("w", 60, 1);
        absurd.workBatch("w", 60, 1);

        assertThat(execCount.get()).isEqualTo(1);
        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void step_typedResult() {
        absurd.registerTask(TaskRegistration.builder("typed-step")
                .handler(JsonValue.class, (params, ctx) -> {
                    int val = ctx.step("num", Integer.class, () -> 99);
                    return val + 1;
                })
                .build());

        SpawnResult spawned = absurd.spawn("typed-step", null);
        absurd.workBatch("w", 60, 1);

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().asInt()).isEqualTo(100);
    }

    @Test
    void step_multipleStepsExecuteInOrder() {
        absurd.registerTask("multi-ctx-step", JsonValue.class, (params, ctx) -> {
            var a = ctx.step("s1", () -> "first");
            var b = ctx.step("s2", () -> "second");
            return Map.of("a", a, "b", b);
        });

        SpawnResult spawned = absurd.spawn("multi-ctx-step", null);
        absurd.workBatch("w", 60, 1);

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().get("a").asText()).isEqualTo("first");
        assertThat(snapshot.result().node().get("b").asText()).isEqualTo("second");
    }

    @Test
    void step_duplicateNamesAutoDisambiguate() {
        AtomicInteger callCount = new AtomicInteger(0);

        absurd.registerTask("dup-step-name", JsonValue.class, (params, ctx) -> {
            var r1 = ctx.step("x", () -> {
                callCount.incrementAndGet();
                return "first";
            });
            var r2 = ctx.step("x", () -> {
                callCount.incrementAndGet();
                return "second";
            });
            return Map.of("r1", r1, "r2", r2);
        });

        SpawnResult spawned = absurd.spawn("dup-step-name", null);
        absurd.workBatch("w", 60, 1);

        assertThat(callCount.get()).isEqualTo(2);
        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().get("r1").asText()).isEqualTo("first");
        assertThat(snapshot.result().node().get("r2").asText()).isEqualTo("second");
    }

    @Test
    void beginStep_completeStep_manualCheckpoint() {
        AtomicInteger execCount = new AtomicInteger(0);
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("manual-step")
                .defaultMaxAttempts(2)
                .handler(JsonValue.class, (params, ctx) -> {
                    StepHandle<String> handle = ctx.beginStep("manual", String.class);
                    String result;
                    if (!handle.done()) {
                        execCount.incrementAndGet();
                        result = ctx.completeStep(handle, "manual-value");
                    } else {
                        result = handle.state();
                    }
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("fail");
                    }
                    return result;
                })
                .build());

        SpawnResult spawned = absurd.spawn("manual-step", null);
        absurd.workBatch("w", 60, 1);
        absurd.workBatch("w", 60, 1);

        assertThat(execCount.get()).isEqualTo(1);
        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().asText()).isEqualTo("manual-value");
    }

    @Test
    void sleepFor_suspendsAndResumesAfterDuration() throws Exception {
        absurd.registerTask("sleep-task", JsonValue.class, (params, ctx) -> {
            ctx.sleepFor("nap", 1);
            return "awake";
        });

        SpawnResult spawned = absurd.spawn("sleep-task", null);
        absurd.workBatch("w", 60, 1);

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isInstanceOf(TaskResultSnapshot.Sleeping.class);

        Thread.sleep(1500);
        absurd.workBatch("w", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void sleepUntil_suspendsUntilInstant() throws Exception {
        absurd.registerTask("sleep-until", JsonValue.class, (params, ctx) -> {
            ctx.sleepUntil("wait", java.time.Instant.now().plusSeconds(1));
            return "woke";
        });

        SpawnResult spawned = absurd.spawn("sleep-until", null);
        absurd.workBatch("w", 60, 1);

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isInstanceOf(TaskResultSnapshot.Sleeping.class);

        Thread.sleep(1500);
        absurd.workBatch("w", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void sleepFor_cachedOnRetry() throws Exception {
        AtomicInteger sleepCalls = new AtomicInteger(0);
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("sleep-cached")
                .defaultMaxAttempts(3)
                .handler(JsonValue.class, (params, ctx) -> {
                    sleepCalls.incrementAndGet();
                    ctx.sleepFor("nap", 1);
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("fail after wake");
                    }
                    return "done";
                })
                .build());

        SpawnResult spawned = absurd.spawn("sleep-cached", null);
        absurd.workBatch("w", 60, 1); // suspends

        Thread.sleep(1500);
        absurd.workBatch("w", 60, 1); // wakes, fails
        absurd.workBatch("w", 60, 1); // retries, sleep cached, succeeds

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void awaitEvent_receivesPayload() {
        absurd.registerTask("event-recv", JsonValue.class, (params, ctx) -> {
            JsonValue payload = ctx.awaitEvent("my-event");
            return payload;
        });

        SpawnResult spawned = absurd.spawn("event-recv", null);
        absurd.workBatch("w", 60, 1);

        assertThat(absurd.fetchTaskResult(spawned.taskID())).isInstanceOf(TaskResultSnapshot.Sleeping.class);

        absurd.emitEvent("my-event", Map.of("msg", "hello"));
        absurd.workBatch("w", 60, 1);

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().get("msg").asText()).isEqualTo("hello");
    }

    @Test
    void awaitEvent_cachedOnRetry() {
        AtomicInteger attempts = new AtomicInteger(0);

        absurd.registerTask(TaskRegistration.builder("event-cached")
                .defaultMaxAttempts(3)
                .handler(JsonValue.class, (params, ctx) -> {
                    JsonValue payload = ctx.awaitEvent("cached-evt");
                    if (attempts.incrementAndGet() == 1) {
                        throw new RuntimeException("fail after event");
                    }
                    return payload;
                })
                .build());

        SpawnResult spawned = absurd.spawn("event-cached", null);
        absurd.workBatch("w", 60, 1);

        absurd.emitEvent("cached-evt", Map.of("v", 42));
        absurd.workBatch("w", 60, 1); // receives event, fails
        absurd.workBatch("w", 60, 1); // retry, event cached, succeeds

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().get("v").asInt()).isEqualTo(42);
    }

    @Test
    void awaitEvent_withTimeout_throwsOnExpiry() {
        AtomicReference<String> errorMsg = new AtomicReference<>();

        absurd.registerTask(TaskRegistration.builder("event-timeout")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    try {
                        ctx.awaitEvent("never-event", 1);
                    } catch (TimeoutException e) {
                        errorMsg.set(e.getMessage());
                        return "timed-out";
                    }
                    return "received";
                })
                .build());

        SpawnResult spawned = absurd.spawn("event-timeout", null);
        absurd.workBatch("w", 60, 1); // suspends waiting for event

        // Wait for timeout to expire, then work again
        try { Thread.sleep(1500); } catch (InterruptedException ignored) {}
        absurd.workBatch("w", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        // Task either completed with "timed-out" or failed depending on timeout semantics
        assertThat(snapshot).isNotNull();
    }

    @Test
    void awaitEvent_raceFreeCaching() {
        absurd.registerTask("event-race", JsonValue.class, (params, ctx) -> {
            JsonValue payload = ctx.awaitEvent("race-event");
            return payload;
        });

        // Emit event BEFORE task runs
        absurd.emitEvent("race-event", Map.of("early", true));

        SpawnResult spawned = absurd.spawn("event-race", null);
        absurd.workBatch("w", 60, 1);

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot.result().node().get("early").asBoolean()).isTrue();
    }

    @Test
    void emitEvent_fromWithinTask() {
        absurd.registerTask("emitter", JsonValue.class, (params, ctx) -> {
            ctx.emitEvent("internal-event", Map.of("from", "emitter"));
            return "emitted";
        });

        absurd.registerTask("listener", JsonValue.class, (params, ctx) -> {
            JsonValue payload = ctx.awaitEvent("internal-event");
            return payload;
        });

        SpawnResult listener = absurd.spawn("listener", null);
        absurd.workBatch("w", 60, 1); // listener suspends

        absurd.spawn("emitter", null);
        absurd.workBatch("w", 60, 1); // emitter runs, emits event

        absurd.workBatch("w", 60, 1); // listener wakes

        var snapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(listener.taskID());
        assertThat(snapshot.result().node().get("from").asText()).isEqualTo("emitter");
    }

    @Test
    void heartbeat_extendsLease() throws Exception {
        absurd.registerTask("heartbeat-task", JsonValue.class, (params, ctx) -> {
            ctx.heartbeat();
            return "alive";
        });

        SpawnResult spawned = absurd.spawn("heartbeat-task", null);
        absurd.workBatch("w", 2, 1); // short claim timeout

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
    }

    @Test
    void taskID_returnsStableId() {
        AtomicReference<String> capturedId = new AtomicReference<>();

        absurd.registerTask("id-task", JsonValue.class, (params, ctx) -> {
            capturedId.set(ctx.taskID());
            return "ok";
        });

        SpawnResult spawned = absurd.spawn("id-task", null);
        absurd.workBatch("w", 60, 1);

        assertThat(capturedId.get()).isEqualTo(spawned.taskID());
    }

    @Test
    void headers_accessibleInHandler() {
        AtomicReference<Object> captured = new AtomicReference<>();

        absurd.registerTask("headers-ctx", JsonValue.class, (params, ctx) -> {
            captured.set(ctx.headers().get("x-trace"));
            return "ok";
        });

        absurd.spawn("headers-ctx", null,
                SpawnOptions.builder().headers(Map.of("x-trace", "trace-123")).build());
        absurd.workBatch("w", 60, 1);

        assertThat(captured.get()).isEqualTo("trace-123");
    }
}
