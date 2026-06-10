package takacsot.absurd.habitat.model;

import java.util.List;

public record TaskListResponse(
    List<TaskSummary> items,
    int total,
    boolean hasMore,
    int page,
    int perPage,
    List<String> availableStatuses,
    List<String> availableQueues,
    List<String> availableTaskNames
) {}
