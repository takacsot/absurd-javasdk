package io.absurd.sdk;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PollIntervalControllerTest {

    private PollIntervalController controller(long minMs, long maxMs, long stepMs,
                                               List<Duration> changes) {
        return new PollIntervalController(
                Duration.ofMillis(minMs),
                Duration.ofMillis(maxMs),
                Duration.ofMillis(stepMs),
                changes != null ? changes::add : null
        );
    }

    @Test
    void startsAtMinInterval() {
        var ctrl = controller(25, 5000, 250, null);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(25);
    }

    @Test
    void fullBatch_returnZeroSleep_andResetsToMin() {
        var ctrl = controller(25, 5000, 250, null);
        // Back off first
        ctrl.afterPoll(0, 10);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(275);

        // Full batch snaps back
        long sleep = ctrl.afterPoll(10, 10);
        assertThat(sleep).isEqualTo(0);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(25);
    }

    @Test
    void partialBatch_resetsToMin_andReturnsSleepDuration() {
        var ctrl = controller(25, 5000, 250, null);
        // Back off first
        ctrl.afterPoll(0, 10);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(275);

        // Partial batch snaps back to min
        long sleep = ctrl.afterPoll(3, 10);
        assertThat(sleep).isEqualTo(25);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(25);
    }

    @Test
    void emptyPoll_backsOffLinearly() {
        var ctrl = controller(25, 5000, 250, null);

        long sleep1 = ctrl.afterPoll(0, 10);
        assertThat(sleep1).isEqualTo(275); // 25 + 250
        assertThat(ctrl.currentIntervalMs()).isEqualTo(275);

        long sleep2 = ctrl.afterPoll(0, 10);
        assertThat(sleep2).isEqualTo(525); // 275 + 250
        assertThat(ctrl.currentIntervalMs()).isEqualTo(525);

        long sleep3 = ctrl.afterPoll(0, 10);
        assertThat(sleep3).isEqualTo(775); // 525 + 250
    }

    @Test
    void emptyPoll_capsAtMax() {
        var ctrl = controller(25, 500, 250, null);

        ctrl.afterPoll(0, 10); // 275
        ctrl.afterPoll(0, 10); // 500 (would be 525 but capped)

        assertThat(ctrl.currentIntervalMs()).isEqualTo(500);

        long sleep = ctrl.afterPoll(0, 10); // still 500
        assertThat(sleep).isEqualTo(500);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(500);
    }

    @Test
    void afterError_backsOff() {
        var ctrl = controller(25, 5000, 250, null);

        long sleep = ctrl.afterError();
        assertThat(sleep).isEqualTo(275);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(275);

        sleep = ctrl.afterError();
        assertThat(sleep).isEqualTo(525);
    }

    @Test
    void afterError_capsAtMax() {
        var ctrl = controller(25, 300, 250, null);

        ctrl.afterError(); // 275
        ctrl.afterError(); // 300 (capped)

        assertThat(ctrl.currentIntervalMs()).isEqualTo(300);

        long sleep = ctrl.afterError();
        assertThat(sleep).isEqualTo(300);
    }

    @Test
    void snapBack_afterBackoff() {
        var ctrl = controller(25, 5000, 250, null);

        // Back off several times
        ctrl.afterPoll(0, 10); // 275
        ctrl.afterPoll(0, 10); // 525
        ctrl.afterPoll(0, 10); // 775

        // Any non-empty claim snaps back
        long sleep = ctrl.afterPoll(1, 10);
        assertThat(sleep).isEqualTo(25);
        assertThat(ctrl.currentIntervalMs()).isEqualTo(25);
    }

    @Test
    void callback_firesOnChange_notOnEveryPoll() {
        var changes = new ArrayList<Duration>();
        var ctrl = controller(25, 5000, 250, changes);

        // First empty poll → changes from 25 to 275
        ctrl.afterPoll(0, 10);
        assertThat(changes).hasSize(1);
        assertThat(changes.get(0)).isEqualTo(Duration.ofMillis(275));

        // Second empty poll → changes from 275 to 525
        ctrl.afterPoll(0, 10);
        assertThat(changes).hasSize(2);
        assertThat(changes.get(1)).isEqualTo(Duration.ofMillis(525));

        // Full batch → changes from 525 to 25
        ctrl.afterPoll(10, 10);
        assertThat(changes).hasSize(3);
        assertThat(changes.get(2)).isEqualTo(Duration.ofMillis(25));

        // Another full batch → no change (already at 25)
        ctrl.afterPoll(10, 10);
        assertThat(changes).hasSize(3);
    }

    @Test
    void callback_notCalledWhenNull() {
        var ctrl = controller(25, 5000, 250, null);
        // Should not throw
        ctrl.afterPoll(0, 10);
        ctrl.afterPoll(5, 10);
        ctrl.afterError();
    }

    @Test
    void batchSizeZero_treatedAsEmptyPoll() {
        var ctrl = controller(25, 5000, 250, null);
        // Edge case: batchSize=0 (shouldn't happen, but handle gracefully)
        long sleep = ctrl.afterPoll(0, 0);
        assertThat(sleep).isEqualTo(275);
    }

    @Test
    void workerOptions_validationRejectsInvalidConfig() {
        // min > max
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
                WorkerOptions.builder()
                        .dynamicPolling(true)
                        .minPollInterval(Duration.ofSeconds(10))
                        .maxPollInterval(Duration.ofSeconds(1))
                        .build()
        );

        // negative step
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
                WorkerOptions.builder()
                        .dynamicPolling(true)
                        .pollBackoffStep(Duration.ofMillis(-1))
                        .build()
        );

        // zero step
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
                WorkerOptions.builder()
                        .dynamicPolling(true)
                        .pollBackoffStep(Duration.ZERO)
                        .build()
        );

        // zero min interval
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () ->
                WorkerOptions.builder()
                        .dynamicPolling(true)
                        .minPollInterval(Duration.ZERO)
                        .build()
        );
    }

    @Test
    void workerOptions_validationSkippedWhenDynamicPollingDisabled() {
        // Should not throw even with nonsensical values when dynamic polling is off
        var opts = WorkerOptions.builder()
                .dynamicPolling(false)
                .minPollInterval(Duration.ofSeconds(99))
                .maxPollInterval(Duration.ofSeconds(1))
                .build();
        assertThat(opts.dynamicPolling()).isFalse();
    }

    @Test
    void workerOptions_defaultsAreReasonable() {
        var opts = WorkerOptions.builder().dynamicPolling(true).build();
        assertThat(opts.minPollInterval()).isEqualTo(Duration.ofMillis(25));
        assertThat(opts.maxPollInterval()).isEqualTo(Duration.ofSeconds(5));
        assertThat(opts.pollBackoffStep()).isEqualTo(Duration.ofMillis(250));
    }
}
