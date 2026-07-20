package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicPollingTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("dynpoll_q");
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
    void dynamicPolling_backsOffWhenIdle_snapsBackOnWork() throws Exception {
        List<Duration> intervalChanges = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch taskDone = new CountDownLatch(1);

        absurd.registerTask("dynpoll-task", JsonValue.class, (params, ctx) -> {
            taskDone.countDown();
            return "ok";
        });

        // Start worker with dynamic polling — queue is empty so it should back off
        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .dynamicPolling(true)
                .minPollInterval(Duration.ofMillis(25))
                .maxPollInterval(Duration.ofMillis(500))
                .pollBackoffStep(Duration.ofMillis(100))
                .onPollIntervalChanged(intervalChanges::add)
                .build());

        // Let the worker idle for a bit — it should back off
        Thread.sleep(400);

        // Verify backoff happened (interval increased from 25)
        assertThat(intervalChanges).isNotEmpty();
        Duration lastBeforeSpawn = intervalChanges.get(intervalChanges.size() - 1);
        assertThat(lastBeforeSpawn.toMillis()).isGreaterThan(25);

        // Now spawn a task — worker should snap back to min
        int changeCountBeforeSpawn = intervalChanges.size();
        absurd.spawn("dynpoll-task", null);

        // Wait for task to complete
        assertThat(taskDone.await(5, TimeUnit.SECONDS)).isTrue();

        // Verify snap-back to min happened
        Thread.sleep(100); // small grace for the callback
        assertThat(intervalChanges.size()).isGreaterThan(changeCountBeforeSpawn);

        // Find the snap-back event (min interval should appear after the backoff)
        boolean snappedBack = intervalChanges.stream()
                .skip(changeCountBeforeSpawn)
                .anyMatch(d -> d.toMillis() == 25);
        assertThat(snappedBack).isTrue();

        worker.close();
    }

    @Test
    void dynamicPolling_fullBatchDoesNotSleep() throws Exception {
        CountDownLatch allDone = new CountDownLatch(5);

        absurd.registerTask("fast-task", JsonValue.class, (params, ctx) -> {
            allDone.countDown();
            return "ok";
        });

        // Spawn tasks before starting worker
        for (int i = 0; i < 5; i++) {
            absurd.spawn("fast-task", null);
        }

        long startTime = System.currentTimeMillis();

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .dynamicPolling(true)
                .minPollInterval(Duration.ofMillis(25))
                .maxPollInterval(Duration.ofSeconds(5))
                .pollBackoffStep(Duration.ofMillis(250))
                .batchSize(5)
                .build());

        assertThat(allDone.await(5, TimeUnit.SECONDS)).isTrue();
        long elapsed = System.currentTimeMillis() - startTime;

        worker.close();

        // With full-batch skip-sleep, all 5 tasks should complete very quickly
        // (much less than batchSize * pollInterval would suggest)
        assertThat(elapsed).isLessThan(2000);
    }

    @Test
    void dynamicPolling_disabled_usesFixedInterval() throws Exception {
        List<Duration> intervalChanges = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch taskDone = new CountDownLatch(1);

        absurd.registerTask("fixed-task", JsonValue.class, (params, ctx) -> {
            taskDone.countDown();
            return "ok";
        });

        absurd.spawn("fixed-task", null);

        // Dynamic polling disabled — callback should never fire
        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .dynamicPolling(false)
                .pollIntervalSeconds(0.05)
                .onPollIntervalChanged(intervalChanges::add)
                .build());

        assertThat(taskDone.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(200);

        worker.close();

        // No interval changes when dynamic polling is off
        assertThat(intervalChanges).isEmpty();
    }
}
