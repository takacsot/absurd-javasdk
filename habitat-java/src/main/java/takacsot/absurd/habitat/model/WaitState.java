package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record WaitState(
    String waitType,
    Instant wakeAt,
    String wakeEvent,
    String stepName,
    JsonNode payload,
    JsonNode eventPayload,
    Instant emittedAt,
    Instant updatedAt
) {}
