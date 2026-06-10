package io.absurd.sdk;

/**
 * Internal exception thrown to suspend a run. Users should never catch this directly.
 */
public final class SuspendTaskException extends RuntimeException {

    public SuspendTaskException() {
        super("Task suspended");
    }
}
