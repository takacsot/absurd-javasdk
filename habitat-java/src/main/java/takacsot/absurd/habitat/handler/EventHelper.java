package takacsot.absurd.habitat.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.Jdbi;
import takacsot.absurd.habitat.SqlUtil;
import takacsot.absurd.habitat.model.QueueEvent;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class EventHelper {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EventHelper() {}

    public static List<QueueEvent> fetchQueueEvents(Jdbi jdbi, String queueName, int limit, String eventName, Instant after, Instant before) {
        if (limit <= 0) limit = 100;
        if (limit > 1000) limit = 1000;

        if (!QueueHelper.queueExists(jdbi, queueName)) return List.of();

        String etable = SqlUtil.queueTable("e", queueName);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT event_name, payload, emitted_at, emitted_at as created_at FROM absurd.").append(etable);
        sql.append(" WHERE payload IS NOT NULL");

        List<Object> params = new ArrayList<>();

        if (eventName != null && !eventName.trim().isEmpty()) {
            params.add(eventName.trim());
            sql.append(" AND event_name = :p").append(params.size());
        }
        if (after != null) {
            params.add(after);
            sql.append(" AND emitted_at >= :p").append(params.size());
        }
        if (before != null) {
            params.add(before);
            sql.append(" AND emitted_at <= :p").append(params.size());
        }

        sql.append(" ORDER BY emitted_at DESC");
        params.add(limit);
        sql.append(" LIMIT :p").append(params.size());

        final List<Object> finalParams = params;
        return jdbi.withHandle(handle -> {
            var query = handle.createQuery(sql.toString());
            for (int i = 0; i < finalParams.size(); i++) {
                query.bind("p" + (i + 1), finalParams.get(i));
            }
            return query.mapToMap().list().stream().map(row -> new QueueEvent(
                queueName,
                (String) row.get("event_name"),
                parseJson(row.get("payload")),
                toInstant(row.get("emitted_at")),
                toInstant(row.get("created_at"))
            )).toList();
        });
    }

    static Instant toInstant(Object v) {
        if (v instanceof OffsetDateTime odt) return odt.toInstant();
        if (v instanceof java.sql.Timestamp ts) return ts.toInstant();
        return null;
    }

    static JsonNode parseJson(Object v) {
        if (v == null) return null;
        try {
            if (v instanceof String s) return MAPPER.readTree(s);
            if (v instanceof byte[] b) return MAPPER.readTree(b);
            return MAPPER.valueToTree(v);
        } catch (Exception e) {
            return null;
        }
    }
}
