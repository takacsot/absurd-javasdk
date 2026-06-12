package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CreateAbsurdTest {

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

    // --- DataSource overloads ---

    @Test
    void createFromDataSource_defaultsQueueToDefault() {
        Absurd a = Absurd.create(dataSource);
        assertThat(a.queueName()).isEqualTo("default");
        a.close();
    }

    @Test
    void createFromDataSource_withQueueName() {
        Absurd a = Absurd.create(dataSource, "my_queue");
        assertThat(a.queueName()).isEqualTo("my_queue");
        a.close();
    }

    @Test
    void createFromDataSource_withMaxAttempts() {
        Absurd a = Absurd.create(dataSource, "my_queue", 10);
        assertThat(a.queueName()).isEqualTo("my_queue");
        a.close();
    }

    // --- Jdbi overloads ---

    @Test
    void createFromJdbi_defaultsQueueToDefault() {
        Absurd a = Absurd.create(Jdbi.create(dataSource));
        assertThat(a.queueName()).isEqualTo("default");
        a.close();
    }

    @Test
    void createFromJdbi_withQueueName() {
        Absurd a = Absurd.create(Jdbi.create(dataSource), "custom");
        assertThat(a.queueName()).isEqualTo("custom");
        a.close();
    }

    @Test
    void createFromJdbi_withMaxAttempts() {
        Absurd a = Absurd.create(Jdbi.create(dataSource), "custom", 3);
        assertThat(a.queueName()).isEqualTo("custom");
        a.close();
    }

    // --- Validation ---

    @Test
    void createWithNullQueueName_throws() {
        assertThatThrownBy(() -> Absurd.create(dataSource, null))
                .isInstanceOf(AbsurdException.class);
    }

    @Test
    void createWithEmptyQueueName_throws() {
        assertThatThrownBy(() -> Absurd.create(dataSource, ""))
                .isInstanceOf(AbsurdException.class);
    }

    @Test
    void createWithTooLongQueueName_throws() {
        String longName = "a".repeat(58);
        assertThatThrownBy(() -> Absurd.create(dataSource, longName))
                .isInstanceOf(AbsurdException.class);
    }

    @Test
    void createWithMaxLengthQueueName_succeeds() {
        String maxName = "a".repeat(57);
        Absurd a = Absurd.create(dataSource, maxName);
        assertThat(a.queueName()).isEqualTo(maxName);
        a.close();
    }
}
