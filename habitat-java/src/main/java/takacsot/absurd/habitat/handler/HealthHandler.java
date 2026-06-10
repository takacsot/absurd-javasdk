package takacsot.absurd.habitat.handler;

import io.javalin.http.Context;
import org.jdbi.v3.core.Jdbi;

public class HealthHandler {

    private final Jdbi jdbi;

    public HealthHandler(Jdbi jdbi) {
        this.jdbi = jdbi;
    }

    public void handle(Context ctx) {
        try {
            jdbi.useHandle(h -> h.execute("SELECT 1"));
            ctx.result("ok");
        } catch (Exception e) {
            ctx.status(503).result("database unavailable");
        }
    }
}
