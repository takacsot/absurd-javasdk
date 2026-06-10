package io.absurd.sdk;

/**
 * Handle returned by {@link TaskContext#beginStep} for decomposed step execution.
 */
public record StepHandle<T>(String name, String checkpointName, boolean done, T state) {

    public static <T> StepHandle<T> completed(String name, String checkpointName, T state) {
        return new StepHandle<>(name, checkpointName, true, state);
    }

    public static <T> StepHandle<T> pending(String name, String checkpointName) {
        return new StepHandle<>(name, checkpointName, false, null);
    }
}
