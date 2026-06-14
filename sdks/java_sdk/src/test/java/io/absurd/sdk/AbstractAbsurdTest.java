package io.absurd.sdk;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.jdbi.v3.core.Jdbi;

import java.nio.file.Files;
import java.nio.file.Path;

abstract class AbstractAbsurdTest {

    static EmbeddedPostgres pg;
    static HikariDataSource dataSource;
    static Absurd absurd;
    static String queueName;

    static void setupBase(String queue) throws Exception {
        pg = EmbeddedPostgres.start();

        HikariConfig config = new HikariConfig();
        config.setDataSource(pg.getPostgresDatabase());
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);

        Path schemaPath = Path.of("../../sql/absurd.sql");
        String schema = Files.readString(schemaPath);
        try (var conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute(schema);
        }

        queueName = queue;
        absurd = Absurd.create(dataSource, queueName);
        absurd.createQueue(queueName);
    }

    static void teardownBase() throws Exception {
        if (absurd != null) absurd.close();
        if (dataSource != null) dataSource.close();
        if (pg != null) pg.close();
    }

    void truncateQueue() {
        Jdbi.create(dataSource).useHandle(h -> {
            try {
                h.execute("TRUNCATE absurd.t_" + queueName + ", absurd.r_" + queueName +
                        ", absurd.c_" + queueName + ", absurd.e_" + queueName +
                        ", absurd.w_" + queueName);
            } catch (Exception ignored) {}
        });
    }
}
