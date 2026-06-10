package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record QueueEvent(
    String queueName,
    String eventName,
    JsonNode payload,
    Instant emittedAt,
    Instant createdAt
) {}
