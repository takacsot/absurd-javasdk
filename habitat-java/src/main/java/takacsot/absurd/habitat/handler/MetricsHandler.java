package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.QueueMetrics;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetricsHandler {

    private static final Logger log = LoggerFactory.getLogger(MetricsHandler.class);
    private final Jdbi jdbi;

    public MetricsHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        Instant now = Instant.now();
        List<QueueMetrics> metrics = new ArrayList<>();

        jdbi.useHandle(handle -> {
            List<String> queueNames = handle
                .createQuery("SELECT queue_name FROM absurd.queues ORDER BY queue_name")
                .mapTo(String.class)
                .list();

            for (String queueName : queueNames) {
                String ttable = SqlUtil.queueTable("t", queueName);
                String rtable = SqlUtil.queueTable("r", queueName);
                try {
                    Map<String, Object> row = handle.createQuery("""
                        SELECT
                            COUNT(*) as total_tasks,
                            COUNT(*) FILTER (WHERE t.state IN ('pending', 'sleeping')) as queued_tasks,
                            COUNT(*) FILTER (WHERE t.state = 'pending' AND r.available_at <= NOW()) as visible_tasks,
                            MIN(CASE WHEN t.state IN ('pending', 'sleeping') THEN r.created_at END) as oldest_at,
                            MAX(CASE WHEN t.state IN ('pending', 'sleeping') THEN r.created_at END) as newest_at
                        FROM absurd.%s t
                        LEFT JOIN absurd.%s r ON r.task_id = t.task_id AND r.run_id = t.last_attempt_run
                        """.formatted(ttable, rtable))
                        .mapToMap()
                        .one();

                    metrics.add(new QueueMetrics(
                        queueName,
                        toLong(row.get("queued_tasks")),
                        toLong(row.get("visible_tasks")),
                        toInstant(row.get("newest_at")),
                        toInstant(row.get("oldest_at")),
                        toLong(row.get("total_tasks")),
                        now
                    ));
                } catch (Exception e) {
                    log.warn("Failed to query metrics for queue {}: {}", queueName, e.getMessage());
                }
            }
        });

        ctx.json(Map.of("queues", metrics));
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) return n.longValue();
        return 0;
    }

    private static Instant toInstant(Object v) {
        if (v instanceof OffsetDateTime odt) return odt.toInstant();
        if (v instanceof java.sql.Timestamp ts) return ts.toInstant();
        return null;
    }
}
