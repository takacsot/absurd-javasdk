# Absurd Java SDK

Java SDK for [Absurd](https://github.com/earendil-works/absurd) — PostgreSQL-native durable task execution.

Uses [JDBI](https://jdbi.org/) for database access.

## Requirements

- Java 17+
- PostgreSQL with the Absurd schema applied

## Installation

Add to your `build.gradle.kts`:

```kotlin
dependencies {
    implementation("io.absurd:absurd-sdk:0.3.0")
}
```

## Quick Start

```java
import io.absurd.sdk.*;
import javax.sql.DataSource;

// Create client
var absurd = Absurd.create(dataSource, "default");
absurd.createQueue();

// Register a task
absurd.registerTask(TaskRegistration.builder("order-fulfillment")
    .handler(OrderParams.class, (params, ctx) -> {
        var payment = ctx.step("process-payment", PaymentResult.class, () -> {
            return new PaymentResult("pay-" + params.orderId(), params.amount());
        });

        var inventory = ctx.step("reserve-inventory", ReserveResult.class, () -> {
            return new ReserveResult(params.items());
        });

        // Wait for external event
        var shipment = ctx.awaitEvent("shipment.packed:" + params.orderId());

        return Map.of(
            "orderId", params.orderId(),
            "trackingNumber", shipment.node().get("trackingNumber").asText()
        );
    })
    .build());

// Start a worker
var worker = absurd.startWorker(WorkerOptions.builder()
    .concurrency(4)
    .build());

// Spawn a task
absurd.spawn("order-fulfillment", new OrderParams("42", 9999, List.of("widget-1")));

// Emit an event
absurd.emitEvent("shipment.packed:42", Map.of("trackingNumber", "TRACK123"));

// Clean up
worker.close();
absurd.close();
```

## Core Concepts

### Tasks and Steps

Tasks are the unit of work. Steps are checkpointed operations within a task — if the
process crashes, execution resumes from the last completed step.

```java
absurd.registerTask(TaskRegistration.builder("my-task")
    .defaultMaxAttempts(3)
    .handler(MyParams.class, (params, ctx) -> {
        // This step runs once; result is cached on retries
        var data = ctx.step("fetch-data", Data.class, () -> fetchFromApi());

        // Multiple steps in sequence
        var processed = ctx.step("process", Result.class, () -> process(data));

        return processed;
    })
    .build());
```

### Events

Tasks can wait for named events. Events use first-write-wins semantics.

```java
// In a task handler:
JsonValue payload = ctx.awaitEvent("order:123:shipped");
JsonValue payload = ctx.awaitEvent("order:123:shipped", 30); // with 30s timeout

// From outside:
absurd.emitEvent("order:123:shipped", Map.of("carrier", "fedex"));
```

### Sleep

Tasks can suspend for a duration or until a specific time.

```java
ctx.sleepFor("cooldown", Duration.ofMinutes(5));
ctx.sleepUntil("scheduled", Instant.parse("2025-01-01T00:00:00Z"));
```

### Workers

Workers poll for tasks and execute them with configurable concurrency.

```java
var worker = absurd.startWorker(WorkerOptions.builder()
    .workerId("my-service:1")
    .concurrency(8)
    .claimTimeout(120)
    .pollIntervalSeconds(0.25)
    .onError(ex -> log.error("Worker error", ex))
    .build());

// Graceful shutdown
worker.close();
```

## Running Tests

Tests require Docker (for testcontainers):

```bash
./gradlew test
```
