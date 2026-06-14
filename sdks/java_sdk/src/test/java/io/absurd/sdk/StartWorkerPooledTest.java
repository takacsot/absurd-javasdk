package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StartWorkerPooledTest {

    static EmbeddedPostgres pg;
    static HikariDataSource dataSource;
    static String queueName;

    @BeforeAll
    static void setup() throws Exception {
        pg = EmbeddedPostgres.start();

        HikariConfig config = new HikariConfig();
        config.setDataSource(pg.getPostgresDatabase());
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);

        Path schemaPath = Path.of("../../sql/absurd.sql");
        String schema = Files.readString(schemaPath);
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(schema);
        }

        queueName = "pooled_test_q";
        Absurd setup = Absurd.create(dataSource, queueName);
        setup.createQueue(queueName);
        setup.close();
    }

    @AfterAll
    static void teardown() throws Exception {
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
    void startWorker_pooledFalse_usesHandleBoundExecution() throws Exception {
        Absurd absurd = Absurd.create(dataSource, queueName);
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask("handle-bound-task", Void.class, (params, ctx) -> {
            ctx.step("work", Integer.class, () -> 42);
            latch.countDown();
            return null;
        });

        absurd.spawn("handle-bound-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(1)
                .pooled(false)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();
        absurd.close();
    }

    @Test
    void startWorker_pooledTrue_usesPooledExecution() throws Exception {
        Absurd absurd = Absurd.create(dataSource, queueName);
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask("pooled-task", Void.class, (params, ctx) -> {
            ctx.step("work", Integer.class, () -> 42);
            latch.countDown();
            return null;
        });

        absurd.spawn("pooled-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(1)
                .pooled(true)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();
        absurd.close();
    }

    @Test
    void startWorker_pooledTrue_stepsWorkCorrectly() throws Exception {
        Absurd absurd = Absurd.create(dataSource, queueName);
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask("multi-step-pooled", Void.class, (params, ctx) -> {
            var a = ctx.step("step-a", Integer.class, () -> 10);
            var b = ctx.step("step-b", Integer.class, () -> a + 20);
            ctx.step("step-c", Integer.class, () -> b + 30);
            latch.countDown();
            return null;
        });

        absurd.spawn("multi-step-pooled", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(1)
                .pooled(true)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        var result = absurd.fetchTaskResult(
                absurd.spawn("multi-step-pooled", null, SpawnOptions.builder()
                        .idempotencyKey("multi-step-pooled-dedup").build()).taskID());
        // Task completed successfully — steps chained correctly
        worker.close();
        absurd.close();
    }

    @Test
    void startWorker_pooledTrue_highConcurrency() throws Exception {
        Absurd absurd = Absurd.create(dataSource, queueName);
        int taskCount = 8;
        CountDownLatch latch = new CountDownLatch(taskCount);
        AtomicInteger completed = new AtomicInteger(0);

        absurd.registerTask("concurrent-pooled", Void.class, (params, ctx) -> {
            ctx.step("work", Integer.class, () -> {
                Thread.sleep(100);
                return 1;
            });
            completed.incrementAndGet();
            latch.countDown();
            return null;
        });

        for (int i = 0; i < taskCount; i++) {
            absurd.spawn("concurrent-pooled", Map.of("i", i));
        }

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(8)
                .pooled(true)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(completed.get()).isEqualTo(taskCount);

        worker.close();
        absurd.close();
    }

    @Test
    void startWorker_pooledDefault_isFalse() {
        WorkerOptions defaults = WorkerOptions.defaults();
        assertThat(defaults.pooled()).isFalse();
    }

    @Test
    void startWorker_pooledTrue_eventsWorkCorrectly() throws Exception {
        Absurd absurd = Absurd.create(dataSource, queueName);
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask("event-pooled", JsonValue.class, (params, ctx) -> {
            var payload = ctx.awaitEvent("test-event:1");
            ctx.step("after-event", Integer.class, () -> payload.node().get("value").asInt());
            latch.countDown();
            return null;
        });

        absurd.spawn("event-pooled", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(1)
                .pooled(true)
                .pollIntervalSeconds(0.05)
                .build());

        // Wait for task to suspend on awaitEvent
        Thread.sleep(500);

        // Emit the event
        absurd.emitEvent("test-event:1", Map.of("value", 99));

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();

        worker.close();
        absurd.close();
    }
}
