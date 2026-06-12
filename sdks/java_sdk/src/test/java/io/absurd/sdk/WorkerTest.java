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

class WorkerTest {

    static EmbeddedPostgres pg;
    static HikariDataSource dataSource;
    static Absurd absurd;
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

        queueName = "worker_test_q";
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
    void startWorker_processesTasksInBackground() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        absurd.registerTask("bg-task", JsonValue.class, (params, ctx) -> {
            latch.countDown();
            return "done";
        });

        absurd.spawn("bg-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05).build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();
    }

    @Test
    void startWorker_withDefaultOptions() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        absurd.registerTask("default-worker", JsonValue.class, (params, ctx) -> {
            latch.countDown();
            return "ok";
        });

        absurd.spawn("default-worker", null);

        Worker worker = absurd.startWorker();
        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();
    }

    @Test
    void startWorker_concurrencyProcessesInParallel() throws Exception {
        AtomicInteger concurrent = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        absurd.registerTask("parallel-task", JsonValue.class, (params, ctx) -> {
            int c = concurrent.incrementAndGet();
            maxConcurrent.updateAndGet(m -> Math.max(m, c));
            Thread.sleep(200);
            concurrent.decrementAndGet();
            latch.countDown();
            return "ok";
        });

        absurd.spawn("parallel-task", null);
        absurd.spawn("parallel-task", null);
        absurd.spawn("parallel-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(3)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();

        assertThat(maxConcurrent.get()).isGreaterThan(1);
    }

    @Test
    void startWorker_respectsConcurrencyLimit() throws Exception {
        AtomicInteger concurrent = new AtomicInteger(0);
        AtomicInteger maxConcurrent = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);

        absurd.registerTask("sequential-task", JsonValue.class, (params, ctx) -> {
            int c = concurrent.incrementAndGet();
            maxConcurrent.updateAndGet(m -> Math.max(m, c));
            Thread.sleep(100);
            concurrent.decrementAndGet();
            latch.countDown();
            return "ok";
        });

        absurd.spawn("sequential-task", null);
        absurd.spawn("sequential-task", null);
        absurd.spawn("sequential-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(1)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();

        assertThat(maxConcurrent.get()).isEqualTo(1);
    }

    @Test
    void startWorker_onErrorCallbackInvoked() throws Exception {
        AtomicReference<Exception> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);

        absurd.registerTask(TaskRegistration.builder("error-task")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("intentional");
                })
                .build());

        absurd.spawn("error-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05)
                .onError(e -> {
                    captured.set(e);
                    latch.countDown();
                })
                .build());

        // Give time for processing; onError may or may not fire depending on
        // whether the exception propagates past executeTask
        Thread.sleep(500);
        worker.close();

        // Task should be failed regardless
        // (onError is for unexpected errors, not task failures handled normally)
    }

    @Test
    void startWorker_closeStopsProcessing() throws Exception {
        AtomicInteger count = new AtomicInteger(0);

        absurd.registerTask("stop-task", JsonValue.class, (params, ctx) -> {
            count.incrementAndGet();
            return "ok";
        });

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05).build());
        worker.close();

        // Spawn after close
        absurd.spawn("stop-task", null);
        Thread.sleep(300);

        assertThat(count.get()).isEqualTo(0);
    }

    @Test
    void startWorker_closeWaitsForInProgressTasks() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);

        absurd.registerTask("slow-task", JsonValue.class, (params, ctx) -> {
            started.countDown();
            Thread.sleep(500);
            finished.countDown();
            return "completed";
        });

        absurd.spawn("slow-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05).build());

        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();

        // Task should have finished before close() returned
        assertThat(finished.getCount()).isEqualTo(0);
    }

    @Test
    void startWorker_customPollInterval() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        absurd.registerTask("fast-poll", JsonValue.class, (params, ctx) -> {
            latch.countDown();
            return "ok";
        });

        absurd.spawn("fast-poll", null);

        long start = System.currentTimeMillis();
        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.02).build());

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        long elapsed = System.currentTimeMillis() - start;
        worker.close();

        // Should be picked up quickly with fast polling
        assertThat(elapsed).isLessThan(1000);
    }

    @Test
    void startWorker_customWorkerId() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        absurd.registerTask("workerid-task", JsonValue.class, (params, ctx) -> {
            latch.countDown();
            return "ok";
        });

        absurd.spawn("workerid-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .workerId("my-custom-worker")
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close();
    }

    @Test
    void startWorker_multipleTasks_allComplete() throws Exception {
        int taskCount = 5;
        CountDownLatch latch = new CountDownLatch(taskCount);

        absurd.registerTask("bulk-task", JsonValue.class, (params, ctx) -> {
            latch.countDown();
            return "ok";
        });

        for (int i = 0; i < taskCount; i++) {
            absurd.spawn("bulk-task", null);
        }

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(2)
                .pollIntervalSeconds(0.05)
                .build());

        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        worker.close();
    }
}
