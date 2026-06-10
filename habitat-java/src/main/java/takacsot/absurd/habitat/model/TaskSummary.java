package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TaskSummary(
    String taskId,
    String runId,
    String queueName,
    String taskName,
    String status,
    int attempt,
    Integer maxAttempts,
    Instant createdAt,
    Instant updatedAt,
    Instant completedAt,
    String workerId,
    JsonNode params
) {}
