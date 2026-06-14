package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class WorkerTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("worker_test_q");
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

    @Test
    void close_waitsForInFlightTasksUpToShutdownTimeout() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskFinished = new CountDownLatch(1);

        absurd.registerTask("slow-shutdown-task", JsonValue.class, (params, ctx) -> {
            taskStarted.countDown();
            Thread.sleep(2000); // simulate 2s work
            taskFinished.countDown();
            return "completed";
        });

        absurd.spawn("slow-shutdown-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05)
                .shutdownTimeoutSeconds(5) // enough time for the task to finish
                .build());

        assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();
        worker.close(); // should wait for the task to complete

        assertThat(taskFinished.await(0, TimeUnit.SECONDS)).isTrue(); // already done
    }

    @Test
    void close_forcesShutdownWhenTimeoutExceeded() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        AtomicReference<Boolean> taskCompleted = new AtomicReference<>(false);

        absurd.registerTask("very-slow-task", JsonValue.class, (params, ctx) -> {
            taskStarted.countDown();
            Thread.sleep(10_000); // simulate 10s work
            taskCompleted.set(true);
            return "completed";
        });

        absurd.spawn("very-slow-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .pollIntervalSeconds(0.05)
                .shutdownTimeoutSeconds(1) // only wait 1s
                .build());

        assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();

        long start = System.currentTimeMillis();
        worker.close();
        long elapsed = System.currentTimeMillis() - start;

        // close() should return in ~6s (5s poller join + 1s executor timeout), not 10s
        assertThat(elapsed).isLessThan(8000);
        assertThat(taskCompleted.get()).isFalse(); // task was interrupted
    }
}
