package io.absurd.sdk;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;

/**
 * A passthrough TaskContext for unit testing task handlers without a database.
 * Steps execute immediately and results are stored in memory.
 */
public class TestTaskContext implements TaskOperations {

    private final String taskId;
    private final Map<String, Object> headers;
    private final Map<String, JsonValue> stepResults = new LinkedHashMap<>();
    private final Map<String, Integer> stepNameCounter = new HashMap<>();
    private final List<EmittedEvent> emittedEvents = new ArrayList<>();
    private final Map<String, JsonValue> eventResponses;

    private TestTaskContext(Builder builder) {
        this.taskId = builder.taskId != null ? builder.taskId : UUID.randomUUID().toString();
        this.headers = builder.headers != null ? Map.copyOf(builder.headers) : Map.of();
        this.eventResponses = builder.eventResponses != null ? new HashMap<>(builder.eventResponses) : new HashMap<>();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String taskID() {
        return taskId;
    }

    @Override
    public Map<String, Object> headers() {
        return headers;
    }

    @Override
    public <T> T step(String name, Class<T> resultType, Callable<T> fn) throws Exception {
        String checkpoint = getCheckpointName(name);
        T result = fn.call();
        stepResults.put(checkpoint, JsonValue.fromObject(result));
        return result;
    }

    @Override
    public JsonValue step(String name, Callable<Object> fn) throws Exception {
        String checkpoint = getCheckpointName(name);
        Object result = fn.call();
        JsonValue value = JsonValue.fromObject(result);
        stepResults.put(checkpoint, value);
        return value;
    }

    @Override
    public <T> StepHandle<T> beginStep(String name, Class<T> resultType) {
        String checkpoint = getCheckpointName(name);
        return StepHandle.pending(name, checkpoint);
    }

    @Override
    public <T> T completeStep(StepHandle<T> stepHandle, T value) {
        stepResults.put(stepHandle.checkpointName(), JsonValue.fromObject(value));
        return value;
    }

    @Override
    public void sleepFor(String stepName, Duration duration) {
        // no-op in tests
    }

    @Override
    public void sleepFor(String stepName, int seconds) {
        // no-op in tests
    }

    @Override
    public void sleepUntil(String stepName, Instant wakeAt) {
        // no-op in tests
    }

    @Override
    public JsonValue awaitEvent(String eventName) {
        return awaitEvent(eventName, null, null);
    }

    @Override
    public JsonValue awaitEvent(String eventName, Integer timeoutSeconds) {
        return awaitEvent(eventName, null, timeoutSeconds);
    }

    @Override
    public JsonValue awaitEvent(String eventName, String stepName, Integer timeoutSeconds) {
        JsonValue response = eventResponses.get(eventName);
        if (response != null) {
            return response;
        }
        throw new TimeoutException("No event response configured for \"" + eventName + "\" in test context");
    }

    @Override
    public void heartbeat() {
        // no-op in tests
    }

    @Override
    public void heartbeat(int seconds) {
        // no-op in tests
    }

    @Override
    public void emitEvent(String eventName, Object payload) {
        emittedEvents.add(new EmittedEvent(eventName, JsonValue.fromObject(payload)));
    }

    @Override
    public void emitEvent(String eventName) {
        emitEvent(eventName, null);
    }

    // --- Test inspection ---

    public Map<String, JsonValue> getStepResults() {
        return Collections.unmodifiableMap(stepResults);
    }

    public List<EmittedEvent> getEmittedEvents() {
        return Collections.unmodifiableList(emittedEvents);
    }

    private String getCheckpointName(String name) {
        int count = stepNameCounter.merge(name, 1, Integer::sum);
        return count == 1 ? name : name + "#" + count;
    }

    public record EmittedEvent(String name, JsonValue payload) {}

    public static final class Builder {
        private String taskId;
        private Map<String, Object> headers;
        private Map<String, JsonValue> eventResponses;

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder headers(Map<String, Object> headers) {
            this.headers = headers;
            return this;
        }

        public Builder eventResponse(String eventName, Object payload) {
            if (eventResponses == null) eventResponses = new HashMap<>();
            eventResponses.put(eventName, JsonValue.fromObject(payload));
            return this;
        }

        public TestTaskContext build() {
            return new TestTaskContext(this);
        }
    }
}
