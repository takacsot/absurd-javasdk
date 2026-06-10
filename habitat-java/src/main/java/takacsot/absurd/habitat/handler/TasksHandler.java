package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.TaskListResponse;
import takacsot.absurd.habitat.model.TaskSummary;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TasksHandler {

    private static final Logger log = LoggerFactory.getLogger(TasksHandler.class);
    private static final List<String> ALL_STATUSES = List.of("pending", "running", "sleeping", "completed", "failed", "cancelled");

    private final Jdbi jdbi;
    private final Map<String, CacheEntry> taskNamesCache = new ConcurrentHashMap<>();

    public TasksHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        String search = trimOrEmpty(ctx.queryParam("q"));
        String statusParam = trimOrEmpty(ctx.queryParam("status"));
        String queueFilter = trimOrEmpty(ctx.queryParam("queue"));
        String taskNameFilter = trimOrEmpty(ctx.queryParam("taskName"));
        String taskIdFilter = trimOrEmpty(ctx.queryParam("taskId"));
        Instant afterTime = parseInstant(ctx.queryParam("after"));
        Instant beforeTime = parseInstant(ctx.queryParam("before"));

        int page = parsePositiveInt(ctx.queryParam("page"), 1);
        int perPage = parsePositiveInt(ctx.queryParam("perPage"), 25);
        if (perPage > 200) perPage = 200;
        if (page < 1) page = 1;

        List<String> queueNames = QueueHelper.listQueueNames(jdbi);

        String statusFilter = normalizeStatus(statusParam);
        if (statusFilter == null && !statusParam.isEmpty()) {
            ctx.json(emptyResponse(page, perPage, queueNames));
            return;
        }
        if (statusFilter == null) statusFilter = "";

        if (!taskIdFilter.isEmpty()) {
            try {
                UUID.fromString(taskIdFilter);
            } catch (IllegalArgumentException e) {
                ctx.json(emptyResponse(page, perPage, queueNames));
                return;
            }
        }

        List<String> selectedQueues = queueNames;
        if (!queueFilter.isEmpty()) {
            selectedQueues = queueNames.stream().filter(q -> q.equals(queueFilter)).toList();
            if (selectedQueues.isEmpty()) {
                ctx.json(emptyResponse(page, perPage, queueNames));
                return;
            }
        }

        List<String> availableTaskNames = listRecentTaskNames(selectedQueues, 5000);

        int start = (page - 1) * perPage;
        int windowSize = start + perPage + 1;
        int limitPerQueue = search.isEmpty() ? windowSize : 0;
        boolean includeParams = !search.isEmpty();

        List<TaskSummary> merged = new ArrayList<>();
        boolean hasWindowTruncation = false;

        for (String queueName : selectedQueues) {
            try {
                FetchResult result = fetchQueueTaskCandidates(
                    queueName, statusFilter, taskNameFilter, taskIdFilter,
                    limitPerQueue, includeParams, afterTime, beforeTime
                );
                merged.addAll(result.tasks);
                if (result.truncated) hasWindowTruncation = true;
            } catch (Exception e) {
                log.warn("Failed to query tasks for queue {}: {}", queueName, e.getMessage());
            }
        }

        if (!search.isEmpty()) {
            String searchLower = search.toLowerCase();
            merged = merged.stream().filter(t -> matchesSearch(t, searchLower)).toList();
        }

        merged = new ArrayList<>(merged);
        merged.sort(Comparator.comparing(TaskSummary::runId).reversed());

        int total = -1;
        if (!search.isEmpty() || !hasWindowTruncation) {
            total = merged.size();
        }

        if (start > merged.size()) start = merged.size();
        int end = Math.min(start + perPage, merged.size());

        boolean hasMore = merged.size() > end || hasWindowTruncation;
        if (total >= 0) hasMore = end < total;

        ctx.json(new TaskListResponse(
            merged.subList(start, end),
            total,
            hasMore,
            page,
            perPage,
            ALL_STATUSES,
            queueNames,
            availableTaskNames
        ));
    }

    private FetchResult fetchQueueTaskCandidates(
        String queueName, String statusFilter, String taskNameFilter, String taskIdFilter,
        int limit, boolean includeParams, Instant afterTime, Instant beforeTime
    ) {
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

        if (!statusFilter.isEmpty()) {
            params.add(statusFilter);
            clauses.add("r.state = :p" + params.size());
        }
        if (!taskNameFilter.isEmpty()) {
            params.add(taskNameFilter);
            clauses.add("t.task_name = :p" + params.size());
        }
        if (!taskIdFilter.isEmpty()) {
            params.add(taskIdFilter);
            clauses.add("t.task_id = :p" + params.size());
        }
        if (afterTime != null) {
            params.add(afterTime);
            clauses.add("r.created_at >= :p" + params.size());
        }
        if (beforeTime != null) {
            params.add(beforeTime);
            clauses.add("r.created_at <= :p" + params.size());
        }

        if (!clauses.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", clauses));
        }

        sql.append(" ORDER BY r.run_id DESC");

        int queryLimit = limit;
        if (queryLimit > 0) {
            queryLimit += 1;
            params.add(queryLimit);
            sql.append(" LIMIT :p").append(params.size());
        }

        final List<Object> finalParams = params;
        List<TaskSummary> tasks = jdbi.withHandle(handle -> {
            var query = handle.createQuery(sql.toString());
            for (int i = 0; i < finalParams.size(); i++) {
                query.bind("p" + (i + 1), finalParams.get(i));
            }
            return query.map(TaskRowMapper.SUMMARY).list();
        });

        boolean truncated = false;
        if (limit > 0 && tasks.size() > limit) {
            tasks = new ArrayList<>(tasks.subList(0, limit));
            truncated = true;
        }

        return new FetchResult(tasks, truncated);
    }

    private List<String> listRecentTaskNames(List<String> queueNames, int recentRunLimit) {
        Set<String> names = new TreeSet<>();
        for (String queueName : queueNames) {
            try {
                names.addAll(getRecentTaskNamesCached(queueName, recentRunLimit));
            } catch (Exception e) {
                log.warn("Failed to list recent task names for queue {}: {}", queueName, e.getMessage());
            }
        }
        return new ArrayList<>(names);
    }

    private List<String> getRecentTaskNamesCached(String queueName, int recentRunLimit) {
        CacheEntry entry = taskNamesCache.get(queueName);
        if (entry != null && Instant.now().isBefore(entry.expiresAt)) {
            return entry.values;
        }

        String ttable = SqlUtil.queueTable("t", queueName);
        String rtable = SqlUtil.queueTable("r", queueName);

        List<String> taskNames = jdbi.withHandle(handle ->
            handle.createQuery("""
                WITH recent_runs AS (
                    SELECT task_id FROM absurd.%s ORDER BY run_id DESC LIMIT :limit
                )
                SELECT DISTINCT t.task_name
                FROM recent_runs r
                JOIN absurd.%s t ON t.task_id = r.task_id
                WHERE t.task_name <> ''
                ORDER BY t.task_name
                """.formatted(rtable, ttable))
                .bind("limit", recentRunLimit)
                .mapTo(String.class)
                .list()
        );

        taskNamesCache.put(queueName, new CacheEntry(taskNames, Instant.now().plusSeconds(60)));
        return taskNames;
    }

    private static boolean matchesSearch(TaskSummary task, String searchLower) {
        return task.taskId().toLowerCase().contains(searchLower)
            || task.runId().toLowerCase().contains(searchLower)
            || task.queueName().toLowerCase().contains(searchLower)
            || task.taskName().toLowerCase().contains(searchLower)
            || (task.params() != null && task.params().toString().toLowerCase().contains(searchLower));
    }

    private static String normalizeStatus(String value) {
        if (value == null || value.trim().isEmpty()) return "";
        String lower = value.trim().toLowerCase();
        return ALL_STATUSES.contains(lower) ? lower : null;
    }

    private static TaskListResponse emptyResponse(int page, int perPage, List<String> queueNames) {
        return new TaskListResponse(List.of(), 0, false, page, perPage, ALL_STATUSES, queueNames, List.of());
    }

    private static String trimOrEmpty(String v) {
        return v == null ? "" : v.trim();
    }

    private static int parsePositiveInt(String value, int fallback) {
        if (value == null || value.isEmpty()) return fallback;
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static Instant parseInstant(String v) {
        if (v == null || v.trim().isEmpty()) return null;
        try { return Instant.parse(v.trim()); } catch (Exception e) { return null; }
    }

    private record FetchResult(List<TaskSummary> tasks, boolean truncated) {}
    private record CacheEntry(List<String> values, Instant expiresAt) {}
}
