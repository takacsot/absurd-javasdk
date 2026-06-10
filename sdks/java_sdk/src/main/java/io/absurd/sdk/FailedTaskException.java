package io.absurd.sdk;

/**
 * Internal exception thrown when the current run has already failed (e.g., claim timeout).
 */
public final class FailedTaskException extends RuntimeException {

    public FailedTaskException() {
        super("Task already failed");
    }
}
