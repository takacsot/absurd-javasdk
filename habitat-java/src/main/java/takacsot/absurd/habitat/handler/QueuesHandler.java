package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.QueueSummary;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueuesHandler {

    private static final Logger log = LoggerFactory.getLogger(QueuesHandler.class);
    private final Jdbi jdbi;

    public QueuesHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        List<QueueSummary> queues = new ArrayList<>();

        jdbi.useHandle(handle -> {
            List<Map<String, Object>> rows = handle
                .createQuery("SELECT queue_name, created_at FROM absurd.queues ORDER BY queue_name")
                .mapToMap()
                .list();

            for (Map<String, Object> row : rows) {
                String queueName = (String) row.get("queue_name");
                Instant createdAt = toInstant(row.get("created_at"));
                String ttable = SqlUtil.queueTable("t", queueName);

                try {
                    Map<String, Object> counts = handle.createQuery("""
                        SELECT
                            COUNT(*) FILTER (WHERE state = 'pending') as pending_count,
                            COUNT(*) FILTER (WHERE state = 'running') as running_count,
                            COUNT(*) FILTER (WHERE state = 'sleeping') as sleeping_count,
                            COUNT(*) FILTER (WHERE state = 'completed') as completed_count,
                            COUNT(*) FILTER (WHERE state = 'failed') as failed_count,
                            COUNT(*) FILTER (WHERE state = 'cancelled') as cancelled_count
                        FROM absurd.%s
                        """.formatted(ttable))
                        .mapToMap()
                        .one();

                    queues.add(new QueueSummary(
                        queueName, createdAt,
                        toLong(counts.get("pending_count")),
                        toLong(counts.get("running_count")),
                        toLong(counts.get("sleeping_count")),
                        toLong(counts.get("completed_count")),
                        toLong(counts.get("failed_count")),
                        toLong(counts.get("cancelled_count"))
                    ));
                } catch (Exception e) {
                    log.warn("Failed to count tasks for queue {}: {}", queueName, e.getMessage());
                }
            }
        });

        ctx.json(queues);
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
