package io.absurd.sdk;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class WorkerImpl implements Worker {

    private static final Logger log = LoggerFactory.getLogger(WorkerImpl.class);

    private final Absurd absurd;
    private final WorkerOptions options;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Semaphore semaphore;
    private final ExecutorService executor;
    private Thread pollerThread;

    WorkerImpl(Absurd absurd, WorkerOptions options) {
        this.absurd = absurd;
        this.options = options;
        this.semaphore = new Semaphore(options.concurrency());
        this.executor = Executors.newFixedThreadPool(options.concurrency());
    }

    void start() {
        pollerThread = new Thread(this::pollLoop, "absurd-worker-" + options.workerId());
        pollerThread.setDaemon(true);
        pollerThread.start();
    }

    private void pollLoop() {
        while (running.get()) {
            try {
                int available = semaphore.availablePermits();
                if (available <= 0) {
                    semaphore.acquire();
                    semaphore.release();
                    continue;
                }

                int toClaim = Math.min(options.effectiveBatchSize(), available);
                var tasks = absurd.claimTasks(toClaim, options.claimTimeout(), options.workerId());

                if (tasks.isEmpty()) {
                    long sleepMs = (long) (options.pollIntervalSeconds() * 1000);
                    Thread.sleep(sleepMs);
                    continue;
                }

                for (var task : tasks) {
                    semaphore.acquire();
                    executor.submit(() -> {
                        try {
                            if (options.pooled()) {
                                absurd.executeTaskPooled(task, options.claimTimeout());
                            } else {
                                absurd.jdbi().useHandle(h ->
                                        absurd.executeTask(h, task, options.claimTimeout())
                                );
                            }
                        } catch (Exception e) {
                            options.onError().accept(e);
                        } finally {
                            semaphore.release();
                        }
                    });
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                options.onError().accept(e);
                try {
                    long sleepMs = (long) (options.pollIntervalSeconds() * 1000);
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    @Override
    public void close() {
        running.set(false);
        if (pollerThread != null) {
            pollerThread.interrupt();
            try {
                pollerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(options.shutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
