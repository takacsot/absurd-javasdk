package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jdbi.v3.core.Jdbi;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class AbsurdDemo {
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
        Absurd absurd = Absurd.create(dataSource, queueName);
        absurd.createQueue(queueName);
//        absurd.registerTask(TaskRegistration.builder("testing")
//                .handler(JsonValue.class, (params, ctx) -> {
//                    System.out.println("............testing task..............");
//                    var result = ctx.step("compute", () -> 42);
//                    return result;
//                })
//                .build());

        for (int i =0;i < 15 ;i++){
            System.out.println("spawning...");
            absurd.spawn("testing", Map.of("hello", "world", "currenttime", "20:13"), SpawnOptions.builder().queue("default").build());
        }
//        absurd.emitEvent("computed-value 2026-06-05T20:52:39.038546", java.util.Map.of("data", "esakkor mi van?"));

//        System.out.println("WORK");
//        CountDownLatch latch = new CountDownLatch(3);
//        AtomicReference<Exception> error = new AtomicReference<>();
//        Worker worker = absurd.startWorker(WorkerOptions.builder()
//                .concurrency(2)
//                .pollIntervalSeconds(0.05)
//                .onError(error::set)
//                .build());
//
//        boolean completed = latch.await(10, TimeUnit.SECONDS);
//        worker.close();
//
////        absurd.workBatch("test-worker", 60, 1);
////        absurd.workBatch("test-worker111", 60, 10);
////        absurd.workBatch("test-worker222", 60, 1);
////        absurd.close();
//
//        System.out.println("/WORK");
    }
}
