package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import takacsot.absurd.habitat.model.QueueEvent;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class EventsHandler {

    private final Jdbi jdbi;

    public EventsHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        int limit = QueueEventsHandler.parsePositiveInt(ctx.queryParam("limit"), 100);
        if (limit > 1000) limit = 1000;

        String queueFilter = trimOrNull(ctx.queryParam("queue"));
        String eventFilter = trimOrNull(ctx.queryParam("eventName"));
        Instant afterTime = parseInstant(ctx.queryParam("after"));
        Instant beforeTime = parseInstant(ctx.queryParam("before"));

        List<QueueEvent> events;

        if (queueFilter != null) {
            events = EventHelper.fetchQueueEvents(jdbi, queueFilter, limit, eventFilter, afterTime, beforeTime);
        } else {
            List<String> queueNames = QueueHelper.listQueueNames(jdbi);
            List<QueueEvent> all = new ArrayList<>();
            for (String queueName : queueNames) {
                try {
                    all.addAll(EventHelper.fetchQueueEvents(jdbi, queueName, limit, eventFilter, afterTime, beforeTime));
                } catch (Exception ignored) {}
            }
            all.sort(Comparator.comparing((QueueEvent e) -> e.emittedAt() != null ? e.emittedAt() : e.createdAt()).reversed());
            events = all.size() > limit ? all.subList(0, limit) : all;
        }

        ctx.json(events);
    }

    private static String trimOrNull(String v) {
        if (v == null) return null;
        v = v.trim();
        return v.isEmpty() ? null : v;
    }

    private static Instant parseInstant(String v) {
        if (v == null || v.trim().isEmpty()) return null;
        try {
            return Instant.parse(v.trim());
        } catch (Exception e) {
            return null;
        }
    }
}
