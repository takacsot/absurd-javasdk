package io.absurd.sdk;

/**
 * Functional interface for task handlers.
 *
 * @param <P> parameter type (deserialized from JSON params)
 * @param <R> return type (serialized to JSON result)
 */
@FunctionalInterface
public interface TaskHandler<P, R> {

    R execute(P params, TaskOperations ctx) throws Exception;
}
