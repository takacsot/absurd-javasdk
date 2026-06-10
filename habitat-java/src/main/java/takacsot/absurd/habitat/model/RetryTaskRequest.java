package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record RetryTaskRequest(
    String taskId,
    String queueName,
    boolean spawnNewTask,
    Integer maxAttempts,
    Integer extraAttempts
) {}
