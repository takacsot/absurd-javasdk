package io.absurd.sdk;

import org.junit.jupiter.api.*;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class AbsurdDebugHelperTest extends AbstractAbsurdTest {

    @BeforeAll
    static void setup() throws Exception {
        setupBase("debug_q");
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
    void forkTaskAtStep_resumesFromCheckpoint() {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicInteger step2Count = new AtomicInteger(0);
        AtomicInteger step3Count = new AtomicInteger(0);

        absurd.registerTask("multi-step-debug", JsonValue.class, (params, ctx) -> {
            var s1 = ctx.step("step-1", () -> {
                step1Count.incrementAndGet();
                return "result-1";
            });
            var s2 = ctx.step("step-2", () -> {
                step2Count.incrementAndGet();
                return "result-2";
            });
            var s3 = ctx.step("step-3", () -> {
                step3Count.incrementAndGet();
                return "result-3";
            });
            return Map.of("s1", s1, "s2", s2, "s3", s3);
        });

        // Run original task to completion
        SpawnResult original = absurd.spawn("multi-step-debug", Map.of("input", "hello"));
        absurd.workBatch("w", 60, 1);

        var snapshot = absurd.fetchTaskResult(original.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        assertThat(step1Count.get()).isEqualTo(1);
        assertThat(step2Count.get()).isEqualTo(1);
        assertThat(step3Count.get()).isEqualTo(1);

        // Fork at step-2: step-1 and step-2 should be skipped, step-3 re-executes
        step1Count.set(0);
        step2Count.set(0);
        step3Count.set(0);

        var helper = new AbsurdDebugHelper(absurd);
        SpawnResult fork = helper.forkTaskAtStep(original.taskID(), queueName, "step-2");

        absurd.workBatch("w", 60, 1);

        var forkSnapshot = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(fork.taskID());
        assertThat(forkSnapshot.result().node().get("s1").asText()).isEqualTo("result-1");
        assertThat(forkSnapshot.result().node().get("s2").asText()).isEqualTo("result-2");
        assertThat(forkSnapshot.result().node().get("s3").asText()).isEqualTo("result-3");

        // step-1 and step-2 were loaded from checkpoint (not re-executed)
        assertThat(step1Count.get()).isEqualTo(0);
        assertThat(step2Count.get()).isEqualTo(0);
        // step-3 re-executed
        assertThat(step3Count.get()).isEqualTo(1);
    }

    @Test
    void forkTaskAtStep_forkAtFirstStep_rerunsAllAfter() {
        AtomicInteger step1Count = new AtomicInteger(0);
        AtomicInteger step2Count = new AtomicInteger(0);

        absurd.registerTask("two-step-debug", JsonValue.class, (params, ctx) -> {
            var s1 = ctx.step("alpha", () -> {
                step1Count.incrementAndGet();
                return "a";
            });
            var s2 = ctx.step("beta", () -> {
                step2Count.incrementAndGet();
                return "b";
            });
            return Map.of("s1", s1, "s2", s2);
        });

        SpawnResult original = absurd.spawn("two-step-debug", null);
        absurd.workBatch("w", 60, 1);

        step1Count.set(0);
        step2Count.set(0);

        // Fork at "alpha" — only "beta" should re-run
        var helper = new AbsurdDebugHelper(absurd);
        SpawnResult fork = helper.forkTaskAtStep(original.taskID(), queueName, "alpha");
        absurd.workBatch("w", 60, 1);

        assertThat(step1Count.get()).isEqualTo(0); // skipped
        assertThat(step2Count.get()).isEqualTo(1); // re-executed

        var result = (TaskResultSnapshot.Completed) absurd.fetchTaskResult(fork.taskID());
        assertThat(result.result().node().get("s1").asText()).isEqualTo("a");
        assertThat(result.result().node().get("s2").asText()).isEqualTo("b");
    }
}
