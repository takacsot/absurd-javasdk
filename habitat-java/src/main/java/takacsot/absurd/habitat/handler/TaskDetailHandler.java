package takacsot.absurd.habitat.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.CheckpointState;
import takacsot.absurd.habitat.model.TaskDetail;
import takacsot.absurd.habitat.model.WaitState;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TaskDetailHandler {

    private static final Logger log = LoggerFactory.getLogger(TaskDetailHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Jdbi jdbi;

    public TaskDetailHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        String runId = ctx.pathParam("runId");
        if (runId.isEmpty()) {
            ctx.status(400).result("run ID required");
            return;
        }

        String queueName = findQueueForRun(runId);
        if (queueName == null) {
            ctx.status(404).result("task not found");
            return;
        }

        String ttable = SqlUtil.queueTable("t", queueName);
        String rtable = SqlUtil.queueTable("r", queueName);
        String queueLiteral = SqlUtil.quoteLiteral(queueName);

        Map<String, Object> row = jdbi.withHandle(h ->
            h.createQuery("""
                SELECT
                    t.task_id, r.run_id, %s AS queue_name, t.task_name, t.state,
                    r.attempt, t.max_attempts, t.params, t.retry_strategy, t.headers,
                    COALESCE(r.failure_reason, r.result) AS state_data,
                    r.created_at,
                    COALESCE(r.completed_at, r.failed_at, r.started_at, r.created_at) AS updated_at,
                    r.completed_at, r.claimed_by
                FROM absurd.%s t
                JOIN absurd.%s r ON r.task_id = t.task_id
                WHERE r.run_id = :runId::uuid
                LIMIT 1
                """.formatted(queueLiteral, ttable, rtable))
                .bind("runId", runId)
                .mapToMap()
                .findOne()
                .orElse(null)
        );

        if (row == null) {
            ctx.status(404).result("task not found");
            return;
        }

        String taskId = row.get("task_id").toString();

        List<CheckpointState> checkpoints = fetchCheckpoints(queueName, taskId, runId);
        List<WaitState> waits = fetchWaits(queueName, runId);

        Integer maxAttempts = row.get("max_attempts") != null ? ((Number) row.get("max_attempts")).intValue() : null;

        ctx.json(new TaskDetail(
            taskId,
            row.get("run_id").toString(),
            (String) row.get("queue_name"),
            (String) row.get("task_name"),
            (String) row.get("state"),
            ((Number) row.get("attempt")).intValue(),
            maxAttempts,
            toInstant(row.get("created_at")),
            toInstant(row.get("updated_at")),
            toInstant(row.get("completed_at")),
            (String) row.get("claimed_by"),
            parseJson(row.get("params")),
            parseJson(row.get("retry_strategy")),
            parseJson(row.get("headers")),
            parseJson(row.get("state_data")),
            checkpoints,
            waits
        ));
    }

    private String findQueueForRun(String runId) {
        List<String> queueNames = QueueHelper.listQueueNames(jdbi);
        for (String queueName : queueNames) {
            String rtable = SqlUtil.queueTable("r", queueName);
            boolean found = jdbi.withHandle(h ->
                h.createQuery("SELECT 1 FROM absurd.%s WHERE run_id = :runId::uuid LIMIT 1".formatted(rtable))
                    .bind("runId", runId)
                    .mapTo(Integer.class)
                    .findOne()
                    .isPresent()
            );
            if (found) return queueName;
        }
        return null;
    }

    private List<CheckpointState> fetchCheckpoints(String queueName, String taskId, String runId) {
        String ctable = SqlUtil.queueTable("c", queueName);
        try {
            return jdbi.withHandle(h ->
                h.createQuery("""
                    SELECT checkpoint_name, state, status, owner_run_id, NULL::timestamptz AS expires_at, updated_at
                    FROM absurd.%s
                    WHERE task_id = :taskId::uuid AND owner_run_id = :runId::uuid
                    ORDER BY updated_at DESC
                    """.formatted(ctable))
                    .bind("taskId", taskId)
                    .bind("runId", runId)
                    .map((rs, ctx) -> new CheckpointState(
                        rs.getString("checkpoint_name"),
                        parseJson(rs.getString("state")),
                        rs.getString("status"),
                        rs.getString("owner_run_id"),
                        toInstant(rs.getTimestamp("expires_at")),
                        rs.getTimestamp("updated_at").toInstant()
                    ))
                    .list()
            );
        } catch (Exception e) {
            log.warn("Failed to query checkpoints: {}", e.getMessage());
            return List.of();
        }
    }

    private List<WaitState> fetchWaits(String queueName, String runId) {
        String rtable = SqlUtil.queueTable("r", queueName);
        String wtable = SqlUtil.queueTable("w", queueName);
        String etable = SqlUtil.queueTable("e", queueName);
        try {
            return jdbi.withHandle(h ->
                h.createQuery("""
                    SELECT
                        CASE
                            WHEN r.wake_event IS NOT NULL THEN 'event'
                            WHEN r.available_at > NOW() THEN 'timer'
                            ELSE 'none'
                        END AS wait_type,
                        r.available_at,
                        r.wake_event,
                        w.step_name,
                        NULL::jsonb AS payload,
                        r.event_payload,
                        w.created_at,
                        e.emitted_at
                    FROM absurd.%s r
                    LEFT JOIN absurd.%s w ON w.run_id = r.run_id
                    LEFT JOIN absurd.%s e ON e.event_name = r.wake_event AND e.payload IS NOT NULL
                    WHERE r.run_id = :runId::uuid AND r.state = 'sleeping'
                    ORDER BY w.created_at DESC
                    """.formatted(rtable, wtable, etable))
                    .bind("runId", runId)
                    .map((rs, ctx) -> new WaitState(
                        rs.getString("wait_type"),
                        toInstant(rs.getTimestamp("available_at")),
                        rs.getString("wake_event"),
                        rs.getString("step_name"),
                        parseJson(rs.getString("payload")),
                        parseJson(rs.getString("event_payload")),
                        toInstant(rs.getTimestamp("emitted_at")),
                        toInstant(rs.getTimestamp("created_at"))
                    ))
                    .list()
            );
        } catch (Exception e) {
            log.warn("Failed to query wait states: {}", e.getMessage());
            return List.of();
        }
    }

    private static Instant toInstant(Object v) {
        if (v == null) return null;
        if (v instanceof OffsetDateTime odt) return odt.toInstant();
        if (v instanceof Timestamp ts) return ts.toInstant();
        return null;
    }

    private static JsonNode parseJson(Object v) {
        if (v == null) return null;
        try {
            if (v instanceof String s) return s.isEmpty() ? null : MAPPER.readTree(s);
            if (v instanceof byte[] b) return b.length == 0 ? null : MAPPER.readTree(b);
            return MAPPER.valueToTree(v);
        } catch (Exception e) {
            return null;
        }
    }
}
