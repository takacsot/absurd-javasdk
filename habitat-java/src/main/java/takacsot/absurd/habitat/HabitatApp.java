package takacsot.absurd.habitat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import io.javalin.rendering.template.JavalinThymeleaf;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;
import org.thymeleaf.templateresolver.FileTemplateResolver;
import org.thymeleaf.templateresolver.ITemplateResolver;
import takacsot.absurd.habitat.handler.*;

public class HabitatApp {

    private static final Logger log = LoggerFactory.getLogger(HabitatApp.class);

    public static void main(String[] args) {
        HabitatConfig cfg = HabitatConfig.fromEnv();

        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(cfg.jdbcUrl());
        if (!cfg.dbUser().isEmpty()) hikari.setUsername(cfg.dbUser());
        if (!cfg.dbPassword().isEmpty()) hikari.setPassword(cfg.dbPassword());
        hikari.setMaximumPoolSize(10);

        HikariDataSource ds = new HikariDataSource(hikari);
        Jdbi jdbi = Jdbi.create(ds);

        jdbi.useHandle(h -> h.execute("SELECT 1"));
        log.info("Database connection verified");

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        ITemplateResolver resolver;
        if (!cfg.templateDir().isEmpty()) {
            FileTemplateResolver fileResolver = new FileTemplateResolver();
            fileResolver.setPrefix(cfg.templateDir().endsWith("/") ? cfg.templateDir() : cfg.templateDir() + "/");
            fileResolver.setSuffix(".html");
            fileResolver.setCharacterEncoding("UTF-8");
            fileResolver.setCacheable(false);
            resolver = fileResolver;
            log.info("Using file template resolver: {}", cfg.templateDir());
        } else {
            ClassLoaderTemplateResolver cpResolver = new ClassLoaderTemplateResolver();
            cpResolver.setPrefix("/templates/");
            cpResolver.setSuffix(".html");
            cpResolver.setCharacterEncoding("UTF-8");
            resolver = cpResolver;
        }

        TemplateEngine templateEngine = new TemplateEngine();
        templateEngine.setTemplateResolver(resolver);

        HealthHandler healthHandler = new HealthHandler(jdbi);
        MetricsHandler metricsHandler = new MetricsHandler(jdbi);
        TasksHandler tasksHandler = new TasksHandler(jdbi);
        TaskDetailHandler taskDetailHandler = new TaskDetailHandler(jdbi);
        RetryTaskHandler retryTaskHandler = new RetryTaskHandler(jdbi);
        QueuesHandler queuesHandler = new QueuesHandler(jdbi);
        QueueTasksHandler queueTasksHandler = new QueueTasksHandler(jdbi);
        QueueEventsHandler queueEventsHandler = new QueueEventsHandler(jdbi);
        EventsHandler eventsHandler = new EventsHandler(jdbi);
        PageHandler pageHandler = new PageHandler(jdbi, tasksHandler);

        int port = cfg.port();
        Javalin app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(mapper, false));
            config.fileRenderer(new JavalinThymeleaf(templateEngine));
            config.requestLogger.http((ctx, ms) ->
                log.info("{} {} -> {} ({} ms)", ctx.method(), ctx.path(), ctx.status(), String.format("%.0f", ms))
            );

            config.routes.exception(Exception.class, (e, ctx) -> {
                var sw = new java.io.StringWriter();
                e.printStackTrace(new java.io.PrintWriter(sw));
                ctx.status(500).html("<pre>" + sw.toString().replace("<", "&lt;") + "</pre>");
            });

            // JSON API routes
            config.routes.get("/_healthz", healthHandler::handle);
            config.routes.get("/api/config", ctx -> ctx.json(new RuntimeConfig(cfg.basePath())));
            config.routes.get("/api/metrics", metricsHandler::handle);
            config.routes.get("/api/tasks", tasksHandler::handle);
            config.routes.post("/api/tasks/retry", retryTaskHandler::handle);
            config.routes.get("/api/tasks/{runId}", taskDetailHandler::handle);
            config.routes.get("/api/queues", queuesHandler::handle);
            config.routes.get("/api/queues/{queue}/tasks", queueTasksHandler::handle);
            config.routes.get("/api/queues/{queue}/events", queueEventsHandler::handle);
            config.routes.get("/api/events", eventsHandler::handle);

            // HTML page routes
            config.routes.get("/", pageHandler::overview);
            config.routes.get("/tasks", pageHandler::tasksPage);
            config.routes.get("/tasks/{taskId}", pageHandler::taskDetail);
            config.routes.get("/queues", pageHandler::queuesPage);
            config.routes.get("/events", pageHandler::eventsPage);
        }).start(port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            app.stop();
            ds.close();
        }));

        log.info("Habitat listening on http://localhost:{}", port);
    }

    record RuntimeConfig(String basePath) {
        public String apiBasePath() {
            return (basePath == null || basePath.isEmpty()) ? "/api" : basePath + "/api";
        }
        public String staticBasePath() {
            return (basePath == null || basePath.isEmpty()) ? "/_static" : basePath + "/_static";
        }
    }
}
