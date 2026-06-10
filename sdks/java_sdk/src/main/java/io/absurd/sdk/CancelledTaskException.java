package io.absurd.sdk;

/**
 * Internal exception thrown when a task has been cancelled. Users should never catch this directly.
 */
public final class CancelledTaskException extends RuntimeException {

    public CancelledTaskException() {
        super("Task cancelled");
    }
}
