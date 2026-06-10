package takacsot.absurd.habitat.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record TaskDetail(
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
    JsonNode params,
    JsonNode retryStrategy,
    JsonNode headers,
    JsonNode state,
    List<CheckpointState> checkpoints,
    List<WaitState> waits
) {
    public String stackTrace() {
        if (state == null) return null;
        JsonNode src = state;
        // Handle JDBI jsonb wrapper: {"type":"jsonb","value":"..."}
        if (src.has("type") && src.has("value") && "jsonb".equals(src.get("type").asText())) {
            try {
                src = new com.fasterxml.jackson.databind.ObjectMapper().readTree(src.get("value").asText());
            } catch (Exception e) {
                return src.get("value").asText();
            }
        }
        // Look for stack trace field
        for (String field : new String[]{"stack", "stackTrace", "stack_trace"}) {
            if (src.has(field)) {
                return src.get(field).asText().replace("\\n", "\n").replace("\\t", "\t");
            }
        }
        // Fallback to message
        if (src.has("message")) return src.get("message").asText();
        return src.isTextual() ? src.asText() : src.toPrettyString();
    }
}
