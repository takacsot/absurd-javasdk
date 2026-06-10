package takacsot.absurd.habitat.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.RetryTaskRequest;
import takacsot.absurd.habitat.model.RetryTaskResponse;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class RetryTaskHandler {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Jdbi jdbi;

    public RetryTaskHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        RetryTaskRequest request = ctx.bodyAsClass(RetryTaskRequest.class);

        if (request.taskId() == null || request.taskId().trim().isEmpty()) {
            ctx.status(400).result("taskId is required");
            return;
        }
        if (request.queueName() == null || request.queueName().trim().isEmpty()) {
            ctx.status(400).result("queueName is required");
            return;
        }

        String taskId;
        try {
            taskId = UUID.fromString(request.taskId()).toString();
        } catch (IllegalArgumentException e) {
            ctx.status(400).result("taskId must be a valid UUID");
            return;
        }

        if (request.maxAttempts() != null && request.maxAttempts() < 1) {
            ctx.status(400).result("maxAttempts must be >= 1");
            return;
        }
        if (request.extraAttempts() != null && request.extraAttempts() < 1) {
            ctx.status(400).result("extraAttempts must be >= 1");
            return;
        }
        if (request.spawnNewTask() && request.extraAttempts() != null) {
            ctx.status(400).result("extraAttempts cannot be used when spawnNewTask is true");
            return;
        }
        if (!request.spawnNewTask() && request.maxAttempts() != null) {
            ctx.status(400).result("maxAttempts cannot be used when spawnNewTask is false; use extraAttempts");
            return;
        }

        if (!QueueHelper.queueExists(jdbi, request.queueName())) {
            ctx.status(404).result("queue not found");
            return;
        }

        Map<String, Object> options = new LinkedHashMap<>();
        if (request.spawnNewTask()) {
            options.put("spawn_new", true);
            if (request.maxAttempts() != null) {
                options.put("max_attempts", request.maxAttempts());
            }
        } else if (request.extraAttempts() != null) {
            int currentAttempts = getTaskAttempts(request.queueName(), taskId);
            if (currentAttempts < 0) {
                ctx.status(404).result("task not found in queue");
                return;
            }
            options.put("max_attempts", currentAttempts + request.extraAttempts());
        }

        try {
            String optionsJson = MAPPER.writeValueAsString(options);
            Map<String, Object> result = jdbi.withHandle(h ->
                h.createQuery("SELECT task_id, run_id, attempt, created FROM absurd.retry_task(:queue, :taskId, :options::jsonb)")
                    .bind("queue", request.queueName())
                    .bind("taskId", taskId)
                    .bind("options", optionsJson)
                    .mapToMap()
                    .one()
            );

            ctx.json(new RetryTaskResponse(
                (String) result.get("task_id"),
                (String) result.get("run_id"),
                ((Number) result.get("attempt")).intValue(),
                (Boolean) result.get("created"),
                request.queueName()
            ));
        } catch (Exception e) {
            ctx.status(400).result(e.getMessage());
        }
    }

    private int getTaskAttempts(String queueName, String taskId) {
        String table = SqlUtil.queueTable("t", queueName);
        return jdbi.withHandle(h ->
            h.createQuery("SELECT attempts FROM absurd.%s WHERE task_id = :taskId::uuid LIMIT 1".formatted(table))
                .bind("taskId", taskId)
                .mapTo(Integer.class)
                .findOne()
                .orElse(-1)
        );
    }
}
