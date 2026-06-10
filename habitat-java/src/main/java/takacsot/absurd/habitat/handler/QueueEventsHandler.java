package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;
import takacsot.absurd.habitat.model.QueueEvent;

import java.util.List;

public class QueueEventsHandler {

    private final Jdbi jdbi;

    public QueueEventsHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        String queueName = ctx.pathParam("queue");
        if (queueName.isEmpty()) {
            ctx.status(400).result("queue name required");
            return;
        }

        int limit = parsePositiveInt(ctx.queryParam("limit"), 100);
        if (limit > 500) limit = 500;
        String eventName = ctx.queryParam("eventName");

        List<QueueEvent> events = EventHelper.fetchQueueEvents(jdbi, queueName, limit, eventName, null, null);
        ctx.json(events);
    }

    static int parsePositiveInt(String value, int fallback) {
        if (value == null || value.isEmpty()) return fallback;
        try {
            int parsed = Integer.parseInt(value);
            return parsed > 0 ? parsed : fallback;
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
