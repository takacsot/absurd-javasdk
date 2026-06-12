package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TaskLifecycleListenerTest {

    static EmbeddedPostgres pg;
    static HikariDataSource dataSource;

    @BeforeAll
    static void setup() throws Exception {
        pg = EmbeddedPostgres.start();

        HikariConfig config = new HikariConfig();
        config.setDataSource(pg.getPostgresDatabase());
        config.setMaximumPoolSize(5);
        dataSource = new HikariDataSource(config);

        Path schemaPath = Path.of("../../sql/absurd.sql");
        String schema = Files.readString(schemaPath);
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(schema);
        }
    }

    @AfterAll
    static void teardown() throws Exception {
        if (dataSource != null) dataSource.close();
        if (pg != null) pg.close();
    }

    @Test
    void onTaskRegistered_fires() {
        List<String> registered = new ArrayList<>();

        Absurd absurd = Absurd.builder(dataSource)
                .queueName("listener_q1")
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskRegistered(String taskName) {
                        registered.add(taskName);
                    }
                })
                .build();
        absurd.createQueue("listener_q1");

        absurd.registerTask("my-task", JsonValue.class, (params, ctx) -> "ok");

        assertThat(registered).containsExactly("my-task");
        absurd.close();
    }

    @Test
    void onTaskStarted_and_onTaskCompleted_fire() {
        List<String> started = new ArrayList<>();
        List<String> completed = new ArrayList<>();

        Absurd absurd = Absurd.builder(dataSource)
                .queueName("listener_q2")
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskStarted(String taskId, String taskName, int attempt) {
                        started.add(taskName + "#" + attempt);
                    }

                    @Override
                    public void onTaskCompleted(String taskId, String taskName, int attempt, long durationMs) {
                        completed.add(taskName + "#" + attempt + ":" + (durationMs >= 0));
                    }
                })
                .build();
        absurd.createQueue("listener_q2");
        absurd.registerTask("tracked", JsonValue.class, (params, ctx) -> "done");

        absurd.spawn("tracked", null);
        absurd.workBatch("w", 60, 1);

        assertThat(started).containsExactly("tracked#1");
        assertThat(completed).containsExactly("tracked#1:true");
        absurd.close();
    }

    @Test
    void onTaskFailed_fires() {
        List<String> failed = new ArrayList<>();

        Absurd absurd = Absurd.builder(dataSource)
                .queueName("listener_q3")
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskFailed(String taskId, String taskName, int attempt, long durationMs, Exception error) {
                        failed.add(taskName + ":" + error.getMessage());
                    }
                })
                .build();
        absurd.createQueue("listener_q3");

        absurd.registerTask(TaskRegistration.builder("fail-tracked")
                .defaultMaxAttempts(1)
                .handler(JsonValue.class, (params, ctx) -> {
                    throw new RuntimeException("boom");
                })
                .build());

        absurd.spawn("fail-tracked", null);
        absurd.workBatch("w", 60, 1);

        assertThat(failed).containsExactly("fail-tracked:boom");
        absurd.close();
    }

    @Test
    void onTaskSuspended_fires() {
        List<String> suspended = new ArrayList<>();

        Absurd absurd = Absurd.builder(dataSource)
                .queueName("listener_q4")
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskSuspended(String taskId, String taskName, int attempt) {
                        suspended.add(taskName);
                    }
                })
                .build();
        absurd.createQueue("listener_q4");

        absurd.registerTask("suspend-tracked", JsonValue.class, (params, ctx) -> {
            ctx.awaitEvent("never");
            return null;
        });

        absurd.spawn("suspend-tracked", null);
        absurd.workBatch("w", 60, 1);

        assertThat(suspended).containsExactly("suspend-tracked");
        absurd.close();
    }

    @Test
    void multipleListeners_allFire() {
        List<String> listener1 = new ArrayList<>();
        List<String> listener2 = new ArrayList<>();

        Absurd absurd = Absurd.builder(dataSource)
                .queueName("listener_q5")
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskCompleted(String taskId, String taskName, int attempt, long durationMs) {
                        listener1.add(taskName);
                    }
                })
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskCompleted(String taskId, String taskName, int attempt, long durationMs) {
                        listener2.add(taskName);
                    }
                })
                .build();
        absurd.createQueue("listener_q5");
        absurd.registerTask("multi-listen", JsonValue.class, (params, ctx) -> "ok");

        absurd.spawn("multi-listen", null);
        absurd.workBatch("w", 60, 1);

        assertThat(listener1).containsExactly("multi-listen");
        assertThat(listener2).containsExactly("multi-listen");
        absurd.close();
    }

    @Test
    void listenerException_doesNotBreakTaskExecution() {
        Absurd absurd = Absurd.builder(dataSource)
                .queueName("listener_q6")
                .listener(new TaskLifecycleListener() {
                    @Override
                    public void onTaskStarted(String taskId, String taskName, int attempt) {
                        throw new RuntimeException("listener error");
                    }
                })
                .build();
        absurd.createQueue("listener_q6");
        absurd.registerTask("robust", JsonValue.class, (params, ctx) -> "ok");

        SpawnResult spawned = absurd.spawn("robust", null);
        absurd.workBatch("w", 60, 1);

        var snapshot = absurd.fetchTaskResult(spawned.taskID());
        assertThat(snapshot).isInstanceOf(TaskResultSnapshot.Completed.class);
        absurd.close();
    }
}
