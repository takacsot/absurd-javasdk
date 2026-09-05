package takacsot.absurd.habitat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.absurd.sdk.Absurd;
import io.absurd.sdk.JsonValue;
import io.absurd.sdk.RetryStrategy;
import io.absurd.sdk.SpawnOptions;
import io.absurd.sdk.TaskRegistration;
import io.absurd.sdk.Worker;
import io.absurd.sdk.WorkerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs the Habitat UI and an Absurd worker in the same JVM, sharing one database.
 *
 * <p>Habitat serves on port 7899 (override with {@code HABITAT_LISTEN}); the worker
 * consumes the {@code default} queue of the same database Habitat displays. A few
 * demo tasks are spawned at startup so the UI has something to show: successful
 * multi-step orders, a durably sleeping task, and a flaky task that recovers via
 * the retry strategy.</p>
 *
 * <p>Run with {@code ./gradlew demo} (uses the same HABITAT_* env vars as HabitatApp).</p>
 */
public class CombinedDemo {

    private static final Logger log = LoggerFactory.getLogger(CombinedDemo.class);

    public static void main(String[] args) throws Exception {
        // 1. Habitat UI -- Javalin starts non-blocking, keeps the JVM alive
        HabitatApp.main(args);

        // 2. Absurd worker on the same database
        HabitatConfig cfg = HabitatConfig.fromEnv();
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(cfg.jdbcUrl());
        if (!cfg.dbUser().isEmpty()) hikari.setUsername(cfg.dbUser());
        if (!cfg.dbPassword().isEmpty()) hikari.setPassword(cfg.dbPassword());
        hikari.setMaximumPoolSize(5);
        HikariDataSource ds = new HikariDataSource(hikari);

        String queue = "default";
        Absurd absurd = Absurd.builder(ds).queueName(queue).build();
        absurd.createQueue(queue);

        // Multi-step task: each step is checkpointed
        absurd.registerTask(TaskRegistration.builder("process-order")
                .handler(JsonValue.class, (params, ctx) -> {
                    var payment = ctx.step("charge-payment", () -> "pay-" + ctx.taskID().substring(0, 8));
                    var label = ctx.step("print-label", () -> "TRACK-" + System.nanoTime());
                    return Map.of("payment", payment.node(), "label", label.node());
                })
                .build());

        // Durable sleep: shows up as "sleeping" in Habitat, resumes on its own
        absurd.registerTask(TaskRegistration.builder("nightly-report")
                .handler(JsonValue.class, (params, ctx) -> {
                    ctx.step("collect", () -> "collected");
                    ctx.sleepFor("wait-a-minute", Duration.ofSeconds(60));
                    return ctx.step("publish", () -> "published").node();
                })
                .build());

        // Flaky task: fails twice, then succeeds -- demonstrates retries in the UI
        AtomicInteger flakyAttempts = new AtomicInteger();
        absurd.registerTask(TaskRegistration.builder("flaky-job")
                .defaultMaxAttempts(5)
                .handler(JsonValue.class, (params, ctx) -> {
                    int attempt = flakyAttempts.incrementAndGet();
                    if (attempt <= 2) {
                        throw new RuntimeException("transient failure on attempt " + attempt);
                    }
                    return "recovered on attempt " + attempt;
                })
                .build());

        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(4)
                .pollIntervalSeconds(1)
                .build());
        log.info("Worker started on queue \"{}\"", queue);

        // 3. Spawn demo work so Habitat has data to display
        for (int i = 1; i <= 5; i++) {
            absurd.spawn("process-order", Map.of("orderId", i));
        }
        absurd.spawn("nightly-report", null);
        absurd.spawn("flaky-job", null, SpawnOptions.builder()
                .retryStrategy(RetryStrategy.fixed(3))
                .build());
        log.info("Spawned 7 demo tasks -- watch them at http://localhost:{}", cfg.port());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down worker...");
            worker.close();
            absurd.close();
            ds.close();
        }));

        Thread.currentThread().join();
    }
}
