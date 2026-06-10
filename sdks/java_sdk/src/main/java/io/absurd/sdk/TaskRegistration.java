package io.absurd.sdk;

/**
 * Defines a task type's configuration and handler.
 *
 * @param name                the unique task name used when spawning and routing to this handler
 * @param queue               the queue this task runs on; {@code null} means use the client's
 *                            default queue. Spawn requests for this task must target this queue
 * @param defaultMaxAttempts  default max attempts for this task type; overrides the client default.
 *                            {@code null} falls through to the client's {@code defaultMaxAttempts}
 * @param defaultCancellation default cancellation policy for tasks of this type; can be overridden
 *                            per-spawn via {@link SpawnOptions#cancellation()}
 * @param paramsType          the Java class to deserialize JSON params into; use {@link JsonValue}
 *                            for dynamic/untyped access
 * @param handler             the function that executes the task logic
 */
/**
 * Defines a task type's configuration and handler.
 *
 * @param name                the unique task name used when spawning and routing to this handler
 * @param queue               the queue this task runs on; {@code null} means use the client's
 *                            default queue. Spawn requests for this task must target this queue
 * @param defaultMaxAttempts  default max attempts for this task type; overrides the client default.
 *                            {@code null} falls through to the client's {@code defaultMaxAttempts}
 * @param defaultCancellation default cancellation policy for tasks of this type; can be overridden
 *                            per-spawn via {@link SpawnOptions#cancellation()}
 * @param paramsType          the Java class to deserialize JSON params into; use {@link JsonValue}
 *                            for dynamic/untyped access
 * @param handler             the function that executes the task logic
 */
public record TaskRegistration(
        String name,
        String queue,
        Integer defaultMaxAttempts,
        CancellationPolicy defaultCancellation,
        Class<?> paramsType,
        TaskHandler<?, ?> handler
) {

    public static Builder builder(String name) {
        return new Builder(name);
    }

    public static final class Builder {
        private final String name;
        private String queue;
        private Integer defaultMaxAttempts;
        private CancellationPolicy defaultCancellation;
        private Class<?> paramsType = JsonValue.class;
        private TaskHandler<?, ?> handler;

        private Builder(String name) {
            this.name = name;
        }

        public Builder queue(String queue) {
            this.queue = queue;
            return this;
        }

        public Builder defaultMaxAttempts(int defaultMaxAttempts) {
            this.defaultMaxAttempts = defaultMaxAttempts;
            return this;
        }

        public Builder defaultCancellation(CancellationPolicy cancellation) {
            this.defaultCancellation = cancellation;
            return this;
        }

        public <P, R> Builder handler(Class<P> paramsType, TaskHandler<P, R> handler) {
            this.paramsType = paramsType;
            this.handler = handler;
            return this;
        }

        public TaskRegistration build() {
            if (handler == null) {
                throw new IllegalStateException("Task handler must be provided");
            }
            return new TaskRegistration(name, queue, defaultMaxAttempts, defaultCancellation, paramsType, handler);
        }
    }
}
