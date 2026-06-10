package takacsot.absurd.habitat.model;

public record RetryTaskResponse(
    String taskId,
    String runId,
    int attempt,
    boolean created,
    String queueName
) {}
