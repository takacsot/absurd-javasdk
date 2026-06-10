package takacsot.absurd.habitat.handler;

import org.jdbi.v3.core.Jdbi;

import java.util.List;

public final class QueueHelper {

    private QueueHelper() {}

    public static List<String> listQueueNames(Jdbi jdbi) {
        return jdbi.withHandle(h ->
            h.createQuery("SELECT queue_name FROM absurd.queues ORDER BY queue_name")
                .mapTo(String.class)
                .list()
        );
    }

    public static boolean queueExists(Jdbi jdbi, String queueName) {
        return jdbi.withHandle(h ->
            h.createQuery("SELECT queue_name FROM absurd.queues WHERE queue_name = :name")
                .bind("name", queueName)
                .mapTo(String.class)
                .findOne()
                .isPresent()
        );
    }
}
