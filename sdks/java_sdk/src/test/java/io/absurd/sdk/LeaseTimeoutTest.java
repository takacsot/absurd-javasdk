package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class LeaseTimeoutTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("lease_q");
    }

    @AfterAll
    static void teardown() throws Exception {
        teardownBase();
    }

    @AfterEach
    void cleanup() {
        truncateQueue();
    }

    @Test
    void fatalOnLeaseTimeout_shutsDownWorkerWhenTaskExceedsLease() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        AtomicBoolean taskInterrupted = new AtomicBoolean(false);

        absurd.registerTask(TaskRegistration.builder("slow-task")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    taskStarted.countDown();
                    try {
                        // Sleep longer than claimTimeout (2s) without heartbeating
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        taskInterrupted.set(true);
                    }
                    return "done";
                })
                .build());

        absurd.spawn("slow-task", null);

        // Start worker with very short claim timeout (2s) and fatalOnLeaseTimeout=true
        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .claimTimeout(2)
                .fatalOnLeaseTimeout(true)
                .build());

        // Wait for task to start
        assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Wait for watchdog to trigger (should fire after 2s)
        Thread.sleep(3000);

        // Worker should have stopped
        assertThat(worker.isRunning()).isFalse();
        assertThat(taskInterrupted.get()).isTrue();

        worker.close();
    }

    @Test
    void fatalOnLeaseTimeout_false_workerContinuesAfterTimeout() throws Exception {
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch taskDone = new CountDownLatch(1);

        absurd.registerTask(TaskRegistration.builder("slow-nonfatal")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    taskStarted.countDown();
                    try {
                        Thread.sleep(3000); // exceeds 2s claim timeout
                    } catch (InterruptedException e) {
                        // not interrupted when fatalOnLeaseTimeout=false
                    }
                    taskDone.countDown();
                    return "done";
                })
                .build());

        absurd.spawn("slow-nonfatal", null);

        // fatalOnLeaseTimeout=false — worker should keep running
        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .claimTimeout(2)
                .fatalOnLeaseTimeout(false)
                .build());

        assertThat(taskStarted.await(5, TimeUnit.SECONDS)).isTrue();

        // Wait for task to complete naturally
        assertThat(taskDone.await(5, TimeUnit.SECONDS)).isTrue();

        // Worker should still be running
        assertThat(worker.isRunning()).isTrue();

        worker.close();
    }

    @Test
    void heartbeat_preventsLeaseTimeout() throws Exception {
        CountDownLatch taskDone = new CountDownLatch(1);

        absurd.registerTask(TaskRegistration.builder("heartbeat-task")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    // Task runs for 3s but heartbeats every 1s (claim timeout is 2s)
                    for (int i = 0; i < 3; i++) {
                        Thread.sleep(1000);
                        ctx.heartbeat();
                    }
                    taskDone.countDown();
                    return "alive";
                })
                .build());

        absurd.spawn("heartbeat-task", null);

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .claimTimeout(2)
                .fatalOnLeaseTimeout(true)
                .build());

        // Task runs 3s total but heartbeats keep lease alive
        assertThat(taskDone.await(6, TimeUnit.SECONDS)).isTrue();

        // Worker should still be running (watchdog was reset by heartbeats)
        assertThat(worker.isRunning()).isTrue();

        worker.close();
    }
}
