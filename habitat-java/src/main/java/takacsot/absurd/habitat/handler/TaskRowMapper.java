package takacsot.absurd.habitat.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import takacsot.absurd.habitat.model.TaskSummary;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;

public class TaskRowMapper implements RowMapper<TaskSummary> {

    public static final TaskRowMapper SUMMARY = new TaskRowMapper();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public TaskSummary map(ResultSet rs, StatementContext ctx) throws SQLException {
        Integer maxAttempts = rs.getObject("max_attempts") != null ? rs.getInt("max_attempts") : null;
        Timestamp completedTs = rs.getTimestamp("completed_at");
        String claimedBy = rs.getString("claimed_by");

        JsonNode params = null;
        try {
            int colCount = rs.getMetaData().getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                if ("params".equalsIgnoreCase(rs.getMetaData().getColumnLabel(i))) {
                    String raw = rs.getString(i);
                    if (raw != null) params = MAPPER.readTree(raw);
                    break;
                }
            }
        } catch (Exception ignored) {}

        return new TaskSummary(
            rs.getString("task_id"),
            rs.getString("run_id"),
            rs.getString("queue_name"),
            rs.getString("task_name"),
            rs.getString("state"),
            rs.getInt("attempt"),
            maxAttempts,
            rs.getTimestamp("created_at").toInstant(),
            rs.getTimestamp("updated_at").toInstant(),
            completedTs != null ? completedTs.toInstant() : null,
            claimedBy,
            params
        );
    }
}
