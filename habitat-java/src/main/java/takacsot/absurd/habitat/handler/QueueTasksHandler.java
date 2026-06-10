package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.TaskSummary;

import java.util.List;

public class QueueTasksHandler {

    private final Jdbi jdbi;

    public QueueTasksHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        String queueName = ctx.pathParam("queue");
        if (queueName.isEmpty()) {
            ctx.status(400).result("queue name required");
            return;
        }

        if (!QueueHelper.queueExists(jdbi, queueName)) {
            ctx.status(404).result("queue not found");
            return;
        }

        String ttable = SqlUtil.queueTable("t", queueName);
        String rtable = SqlUtil.queueTable("r", queueName);
        String queueLiteral = SqlUtil.quoteLiteral(queueName);

        List<TaskSummary> tasks = jdbi.withHandle(handle ->
            handle.createQuery("""
                SELECT
                    t.task_id, r.run_id, %s AS queue_name, t.task_name, r.state,
                    r.attempt, t.max_attempts,
                    r.created_at,
                    COALESCE(r.completed_at, r.failed_at, r.started_at, r.created_at) AS updated_at,
                    r.completed_at, r.claimed_by
                FROM absurd.%s t
                JOIN absurd.%s r ON r.task_id = t.task_id
                ORDER BY r.created_at DESC
                """.formatted(queueLiteral, ttable, rtable))
                .map(TaskRowMapper.SUMMARY)
                .list()
        );

        ctx.json(tasks);
    }
}
