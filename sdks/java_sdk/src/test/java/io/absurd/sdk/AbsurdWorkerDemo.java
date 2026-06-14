package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.time.Duration;

public class AbsurdWorkerDemo {
    public static void main(String[] args) throws Exception {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://localhost:5432/takacso?sslmode=disable");
//        config.setUsername(postgres.getUsername());
//        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(5);
        DataSource dataSource = new HikariDataSource(config);

        // Load schema
//        Path schemaPath = Path.of("../../sql/absurd.sql");
//        String schema = Files.readString(schemaPath);
//        Jdbi jdbi = Jdbi.create(dataSource);
//        jdbi.useHandle(h -> h.createScript(schema).execute());

        String queueName = "default";
        Absurd absurd = Absurd.builder(dataSource)
                .queueName(queueName)
                .build();
        absurd.createQueue(queueName);
        absurd.registerTask(TaskRegistration.builder("testing")
                .handler(JsonValue.class, (params, ctx) -> {
                    System.out.println("............testing task..............");
                    ctx.step("pre-compute", () -> 42);
//                    System.out.println("...Sleeping 2 minutes...");
//                    Thread.sleep(Duration.ofSeconds(120).toMillis());
                    Thread.sleep(Duration.ofSeconds(30).toMillis());
                    var result = ctx.step("compute", () -> 42);
                    return result;
                })
                .build());

        System.out.println("Worker starting...");
        Worker worker = absurd.startWorker(WorkerOptions.builder()
                .concurrency(10)
                .pollIntervalSeconds(3)
                .shutdownTimeoutSeconds(10)
                .build());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down worker...");
            worker.close();
            absurd.close();
            System.out.println("Worker stopped.");
        }));

        System.out.println("Worker running. Press Ctrl+C to stop.");
        Thread.currentThread().join();
    }
}
