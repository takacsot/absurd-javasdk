package io.absurd.sdk;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Public surface of task execution context. Allows test implementations
 * that skip database interaction.
 */
public interface TaskOperations {

    String taskID();

    Map<String, Object> headers();

    <T> T step(String name, Class<T> resultType, Callable<T> fn) throws Exception;

    JsonValue step(String name, Callable<Object> fn) throws Exception;

    <T> StepHandle<T> beginStep(String name, Class<T> resultType);

    <T> T completeStep(StepHandle<T> stepHandle, T value);

    void sleepFor(String stepName, Duration duration);

    void sleepFor(String stepName, int seconds);

    void sleepUntil(String stepName, Instant wakeAt);

    JsonValue awaitEvent(String eventName);

    JsonValue awaitEvent(String eventName, Integer timeoutSeconds);

    JsonValue awaitEvent(String eventName, String stepName, Integer timeoutSeconds);

    void heartbeat();

    void heartbeat(int seconds);

    void emitEvent(String eventName, Object payload);

    void emitEvent(String eventName);

    TaskResultSnapshot awaitTaskResult(String taskID, String queue, Integer timeoutSeconds);
}
