package takacsot.absurd.habitat.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.*;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

public class PageHandler {

    private static final Logger log = LoggerFactory.getLogger(PageHandler.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final List<String> ALL_STATUSES = List.of("pending", "running", "sleeping", "completed", "failed", "cancelled");

    private final Jdbi jdbi;
    private final TasksHandler tasksHandler;

    public PageHandler(Jdbi jdbi, TasksHandler tasksHandler) {
        this.jdbi = jdbi;
        this.tasksHandler = tasksHandler;
    }

    public void overview(Context ctx) {
        List<QueueMetrics> queues = new ArrayList<>();
        Instant now = Instant.now();
        long totalMessages = 0, totalPending = 0, totalVisible = 0;

        try {
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

                        queues.add(new QueueMetrics(
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
        } catch (Exception e) {
            log.warn("Failed to query overview: {}", e.getMessage());
        }

        for (QueueMetrics q : queues) {
            totalMessages += q.totalMessages();
            totalPending += q.queueLength();
            totalVisible += q.queueVisibleLength();
        }

        ctx.attribute("queues", queues);
        ctx.attribute("stats", Map.of(
            "queueCount", queues.size(),
            "totalMessages", totalMessages,
            "totalPending", totalPending,
            "totalVisible", totalVisible
        ));
        ctx.render("overview.html");
    }

    public void tasksPage(Context ctx) {
        String q = trimOrEmpty(ctx.queryParam("q"));
        String queue = trimOrEmpty(ctx.queryParam("queue"));
        String status = trimOrEmpty(ctx.queryParam("status"));
        String taskName = trimOrEmpty(ctx.queryParam("taskName"));
        int page = parsePositiveInt(ctx.queryParam("page"), 1);

        // Fetch via internal API endpoint
        String apiUrl = "/api/tasks?page=" + page + "&perPage=25";
        if (!q.isEmpty()) apiUrl += "&q=" + urlEncode(q);
        if (!queue.isEmpty()) apiUrl += "&queue=" + urlEncode(queue);
        if (!status.isEmpty()) apiUrl += "&status=" + urlEncode(status);
        if (!taskName.isEmpty()) apiUrl += "&taskName=" + urlEncode(taskName);

        // We can't call ourselves, so query JDBI directly (reuse helper logic)
        TaskListResponse response = fetchTaskList(q, queue, status, taskName, page, 25);

        // Available task names: unfiltered except by queue
        List<String> availableTaskNames = fetchAvailableTaskNames(queue);

        ctx.attribute("items", response.items());
        ctx.attribute("total", response.total());
        ctx.attribute("hasMore", response.hasMore());
        ctx.attribute("page", response.page());
        ctx.attribute("pageStart", response.items().isEmpty() ? 0 : (page - 1) * 25 + 1);
        ctx.attribute("pageEnd", response.items().isEmpty() ? 0 : (page - 1) * 25 + response.items().size());
        ctx.attribute("availableStatuses", response.availableStatuses());
        ctx.attribute("availableQueues", response.availableQueues());
        ctx.attribute("availableTaskNames", availableTaskNames);
        ctx.attribute("q", q);
        ctx.attribute("queue", queue);
        ctx.attribute("status", status);
        ctx.attribute("taskName", taskName);
        ctx.attribute("queueParam", queue.isEmpty() ? "" : "&queue=" + urlEncode(queue));
        ctx.attribute("statusParam", status.isEmpty() ? "" : "&status=" + urlEncode(status));
        ctx.attribute("taskNameParam", taskName.isEmpty() ? "" : "&taskName=" + urlEncode(taskName));
        ctx.attribute("searchParam", q.isEmpty() ? "" : "&q=" + urlEncode(q));
        ctx.render("tasks.html");
    }

    public void taskDetail(Context ctx) {
        String taskId = ctx.pathParam("taskId");

        // Fetch all runs for this task via the API task list endpoint
        TaskListResponse response = fetchTaskList("", "", "", "", 1, 200);
        List<TaskSummary> matchingRuns = response.items().stream()
            .filter(t -> t.taskId().equals(taskId))
            .toList();

        if (matchingRuns.isEmpty()) {
            // Try harder with taskId filter
            response = fetchTaskListByTaskId(taskId);
            matchingRuns = response.items();
        }

        String taskNameStr = matchingRuns.isEmpty() ? null : matchingRuns.getFirst().taskName();
        String taskStatus = matchingRuns.isEmpty() ? null : matchingRuns.getFirst().status();

        // Fetch details for each run
        List<TaskDetail> runs = new ArrayList<>();
        for (TaskSummary summary : matchingRuns) {
            TaskDetail detail = fetchTaskDetail(summary.runId());
            if (detail != null) {
                runs.add(detail);
            }
        }

        ctx.attribute("taskId", taskId);
        ctx.attribute("taskName", taskNameStr);
        ctx.attribute("taskStatus", taskStatus);
        ctx.attribute("runs", runs);
        ctx.attribute("checkpoints", runs.stream()
            .flatMap(r -> r.checkpoints().stream())
            .toList());
        ctx.render("task-detail.html");
    }

    public void queuesPage(Context ctx) {
        List<QueueSummary> queues = new ArrayList<>();

        try {
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
        } catch (Exception e) {
            log.warn("Failed to query queues: {}", e.getMessage());
        }

        ctx.attribute("queues", queues);
        ctx.render("queues.html");
    }

    public void eventsPage(Context ctx) {
        String queue = trimOrEmpty(ctx.queryParam("queue"));
        String eventName = trimOrEmpty(ctx.queryParam("eventName"));

        List<String> availableQueues = QueueHelper.listQueueNames(jdbi);
        List<QueueEvent> events;

        if (!queue.isEmpty()) {
            events = EventHelper.fetchQueueEvents(jdbi, queue, 100, eventName.isEmpty() ? null : eventName, null, null);
        } else {
            events = new ArrayList<>();
            for (String qn : availableQueues) {
                try {
                    events.addAll(EventHelper.fetchQueueEvents(jdbi, qn, 100, eventName.isEmpty() ? null : eventName, null, null));
                } catch (Exception ignored) {}
            }
            events.sort(Comparator.comparing((QueueEvent e) -> e.emittedAt() != null ? e.emittedAt() : e.createdAt()).reversed());
            if (events.size() > 100) events = events.subList(0, 100);
        }

        ctx.attribute("events", events);
        ctx.attribute("availableQueues", availableQueues);
        ctx.attribute("queue", queue);
        ctx.attribute("eventName", eventName);
        ctx.render("events.html");
    }

    private TaskListResponse fetchTaskList(String q, String queue, String status, String taskName, int page, int perPage) {
        List<String> queueNames = QueueHelper.listQueueNames(jdbi);
        List<String> selectedQueues = queueNames;
        if (!queue.isEmpty()) {
            selectedQueues = queueNames.stream().filter(qn -> qn.equals(queue)).toList();
        }

        List<TaskSummary> merged = new ArrayList<>();
        for (String qn : selectedQueues) {
            merged.addAll(fetchQueueTasks(qn, status, taskName, "", !q.isEmpty()));
        }

        if (!q.isEmpty()) {
            String searchLower = q.toLowerCase();
            merged = merged.stream().filter(t ->
                t.taskId().toLowerCase().contains(searchLower)
                    || t.runId().toLowerCase().contains(searchLower)
                    || t.queueName().toLowerCase().contains(searchLower)
                    || t.taskName().toLowerCase().contains(searchLower)
                    || (t.params() != null && t.params().toString().toLowerCase().contains(searchLower))
            ).toList();
        }

        merged = new ArrayList<>(merged);
        merged.sort(Comparator.comparing(TaskSummary::runId).reversed());

        int total = merged.size();
        int start = Math.min((page - 1) * perPage, total);
        int end = Math.min(start + perPage, total);

        return new TaskListResponse(
            merged.subList(start, end), total, end < total, page, perPage,
            ALL_STATUSES, queueNames, List.of()
        );
    }

    private List<CheckpointState> fetchCheckpoints(String queueName, String taskId, String runId) {
        String ctable = SqlUtil.queueTable("c", queueName);
        try {
            return jdbi.withHandle(h ->
                h.createQuery("""
                    SELECT checkpoint_name, state, status, owner_run_id, updated_at
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
                        null,
                        rs.getTimestamp("updated_at").toInstant()
                    ))
                    .list()
            );
        } catch (Exception e) {
            log.warn("Failed to query checkpoints for queue {}: {}", queueName, e.getMessage());
            return List.of();
        }
    }

    private List<String> fetchAvailableTaskNames(String queue) {
        List<String> queueNames = queue.isEmpty()
            ? QueueHelper.listQueueNames(jdbi)
            : List.of(queue);
        Set<String> names = new TreeSet<>();
        for (String qn : queueNames) {
            String ttable = SqlUtil.queueTable("t", qn);
            try {
                names.addAll(jdbi.withHandle(h ->
                    h.createQuery("SELECT DISTINCT task_name FROM absurd." + ttable)
                        .mapTo(String.class)
                        .list()
                ));
            } catch (Exception e) {
                log.warn("Failed to fetch task names for queue {}: {}", qn, e.getMessage());
            }
        }
        return List.copyOf(names);
    }

    private TaskListResponse fetchTaskListByTaskId(String taskId) {
        List<String> queueNames = QueueHelper.listQueueNames(jdbi);
        List<TaskSummary> merged = new ArrayList<>();
        for (String qn : queueNames) {
            merged.addAll(fetchQueueTasks(qn, "", "", taskId, false));
        }
        merged.sort(Comparator.comparing(TaskSummary::runId).reversed());
        return new TaskListResponse(merged, merged.size(), false, 1, 200, ALL_STATUSES, queueNames, List.of());
    }

    private List<TaskSummary> fetchQueueTasks(String queueName, String statusFilter, String taskNameFilter, String taskIdFilter, boolean includeParams) {
        String ttable = SqlUtil.queueTable("t", queueName);
        String rtable = SqlUtil.queueTable("r", queueName);
        String queueLiteral = SqlUtil.quoteLiteral(queueName);
        String paramsSelect = includeParams ? "t.params" : "NULL::jsonb";

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT t.task_id, r.run_id, ").append(queueLiteral).append(" AS queue_name, ");
        sql.append("t.task_name, r.state, r.attempt, t.max_attempts, r.created_at, ");
        sql.append("COALESCE(r.completed_at, r.failed_at, r.started_at, r.created_at) AS updated_at, ");
        sql.append("r.completed_at, r.claimed_by, ").append(paramsSelect).append(" AS params ");
        sql.append("FROM absurd.").append(rtable).append(" r ");
        sql.append("JOIN absurd.").append(ttable).append(" t ON t.task_id = r.task_id");

        List<Object> params = new ArrayList<>();
        List<String> clauses = new ArrayList<>();

        if (!statusFilter.isEmpty()) { params.add(statusFilter); clauses.add("r.state = :p" + params.size()); }
        if (!taskNameFilter.isEmpty()) { params.add(taskNameFilter); clauses.add("t.task_name = :p" + params.size()); }
        if (!taskIdFilter.isEmpty()) { params.add(taskIdFilter); clauses.add("t.task_id = :p" + params.size()); }

        if (!clauses.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ", clauses));
        sql.append(" ORDER BY r.run_id DESC LIMIT 500");

        final List<Object> finalParams = params;
        try {
            return jdbi.withHandle(handle -> {
                var query = handle.createQuery(sql.toString());
                for (int i = 0; i < finalParams.size(); i++) {
                    query.bind("p" + (i + 1), finalParams.get(i));
                }
                return query.map(TaskRowMapper.SUMMARY).list();
            });
        } catch (Exception e) {
            log.warn("Failed to query tasks for queue {}: {}", queueName, e.getMessage());
            return List.of();
        }
    }

    private TaskDetail fetchTaskDetail(String runId) {
        String queueName = null;
        List<String> queueNames = QueueHelper.listQueueNames(jdbi);
        for (String qn : queueNames) {
            String rtable = SqlUtil.queueTable("r", qn);
            boolean found = jdbi.withHandle(h ->
                h.createQuery("SELECT 1 FROM absurd.%s WHERE run_id = :runId::uuid LIMIT 1".formatted(rtable))
                    .bind("runId", runId)
                    .mapTo(Integer.class)
                    .findOne()
                    .isPresent()
            );
            if (found) { queueName = qn; break; }
        }
        if (queueName == null) return null;

        String ttable = SqlUtil.queueTable("t", queueName);
        String rtable = SqlUtil.queueTable("r", queueName);
        String queueLiteral = SqlUtil.quoteLiteral(queueName);

        Map<String, Object> row = jdbi.withHandle(h ->
            h.createQuery("""
                SELECT t.task_id, r.run_id, %s AS queue_name, t.task_name, r.state,
                    r.attempt, t.max_attempts, t.params, t.retry_strategy, t.headers,
                    COALESCE(r.failure_reason, r.result) AS state_data,
                    r.created_at, COALESCE(r.completed_at, r.failed_at, r.started_at, r.created_at) AS updated_at,
                    r.completed_at, r.claimed_by
                FROM absurd.%s t JOIN absurd.%s r ON r.task_id = t.task_id WHERE r.run_id = :runId::uuid LIMIT 1
                """.formatted(queueLiteral, ttable, rtable))
                .bind("runId", runId)
                .mapToMap()
                .findOne()
                .orElse(null)
        );
        if (row == null) return null;

        String taskId2 = row.get("task_id").toString();
        List<CheckpointState> checkpoints = fetchCheckpoints(queueName, taskId2, runId);

        Integer maxAttempts = row.get("max_attempts") != null ? ((Number) row.get("max_attempts")).intValue() : null;
        return new TaskDetail(
            taskId2, row.get("run_id").toString(), (String) row.get("queue_name"),
            (String) row.get("task_name"), (String) row.get("state"),
            ((Number) row.get("attempt")).intValue(), maxAttempts,
            toInstant(row.get("created_at")), toInstant(row.get("updated_at")), toInstant(row.get("completed_at")),
            (String) row.get("claimed_by"),
            parseJson(row.get("params")), parseJson(row.get("retry_strategy")),
            parseJson(row.get("headers")), parseJson(row.get("state_data")),
            checkpoints, List.of()
        );
    }

    private static long toLong(Object v) { return v instanceof Number n ? n.longValue() : 0; }
    private static Instant toInstant(Object v) {
        if (v instanceof OffsetDateTime odt) return odt.toInstant();
        if (v instanceof java.sql.Timestamp ts) return ts.toInstant();
        return null;
    }
    private static JsonNode parseJson(Object v) {
        if (v == null) return null;
        try {
            if (v instanceof String s) return s.isEmpty() ? null : MAPPER.readTree(s);
            if (v instanceof byte[] b) return b.length == 0 ? null : MAPPER.readTree(b);
            return MAPPER.valueToTree(v);
        } catch (Exception e) { return null; }
    }
    private static String trimOrEmpty(String v) { return v == null ? "" : v.trim(); }
    private static int parsePositiveInt(String v, int fallback) {
        if (v == null || v.isEmpty()) return fallback;
        try { int p = Integer.parseInt(v); return p > 0 ? p : fallback; } catch (NumberFormatException e) { return fallback; }
    }
    private static String urlEncode(String v) {
        try { return java.net.URLEncoder.encode(v, "UTF-8"); } catch (Exception e) { return v; }
    }
}
