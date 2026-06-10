package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueueMetrics(
    String queueName,
    long queueLength,
    long queueVisibleLength,
    Instant newestMsgAt,
    Instant oldestMsgAt,
    long totalMessages,
    Instant scrapeTime
) {}
