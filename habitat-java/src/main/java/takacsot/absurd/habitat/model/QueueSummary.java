package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueueSummary(
    String queueName,
    Instant createdAt,
    long pendingCount,
    long runningCount,
    long sleepingCount,
    long completedCount,
    long failedCount,
    long cancelledCount
) {}
