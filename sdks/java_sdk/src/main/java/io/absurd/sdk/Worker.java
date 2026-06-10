package io.absurd.sdk;

/**
 * A running background worker that processes tasks from a queue.
 */
public interface Worker extends AutoCloseable {

    /**
     * Signals the worker to stop and waits for in-progress tasks to complete.
     */
    @Override
    void close();
}
