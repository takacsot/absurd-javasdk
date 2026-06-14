package io.absurd.sdk;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TestTaskContextTest {

    @Test
    void step_executesAndStoresResult() throws Exception {
        var ctx = TestTaskContext.builder().build();

        Integer result = ctx.step("add", Integer.class, () -> 1 + 2);

        assertEquals(3, result);
        assertEquals(3, ctx.getStepResults().get("add").as(Integer.class));
    }

    @Test
    void step_jsonValueVariant() throws Exception {
        var ctx = TestTaskContext.builder().build();

        JsonValue result = ctx.step("compute", () -> Map.of("key", "value"));

        assertEquals("value", result.node().get("key").asText());
    }

    @Test
    void step_duplicateNamesGetSuffix() throws Exception {
        var ctx = TestTaskContext.builder().build();

        ctx.step("fetch", Integer.class, () -> 1);
        ctx.step("fetch", Integer.class, () -> 2);

        var results = ctx.getStepResults();
        assertEquals(1, results.get("fetch").as(Integer.class));
        assertEquals(2, results.get("fetch#2").as(Integer.class));
    }

    @Test
    void beginStep_andCompleteStep() {
        var ctx = TestTaskContext.builder().build();

        StepHandle<String> handle = ctx.beginStep("greet", String.class);
        assertFalse(handle.done());

        String result = ctx.completeStep(handle, "hello");

        assertEquals("hello", result);
        assertEquals("hello", ctx.getStepResults().get("greet").as(String.class));
    }

    @Test
    void awaitEvent_returnsConfiguredResponse() {
        var ctx = TestTaskContext.builder()
                .eventResponse("order.shipped", Map.of("carrier", "fedex"))
                .build();

        JsonValue payload = ctx.awaitEvent("order.shipped");

        assertEquals("fedex", payload.node().get("carrier").asText());
    }

    @Test
    void awaitEvent_throwsWhenNotConfigured() {
        var ctx = TestTaskContext.builder().build();

        assertThrows(TimeoutException.class, () -> ctx.awaitEvent("unknown.event"));
    }

    @Test
    void emitEvent_recordsEmissions() {
        var ctx = TestTaskContext.builder().build();

        ctx.emitEvent("task.done", Map.of("status", "ok"));
        ctx.emitEvent("notify");

        var events = ctx.getEmittedEvents();
        assertEquals(2, events.size());
        assertEquals("task.done", events.get(0).name());
        assertEquals("ok", events.get(0).payload().node().get("status").asText());
        assertEquals("notify", events.get(1).name());
    }

    @Test
    void sleepFor_isNoOp() {
        var ctx = TestTaskContext.builder().build();
        assertDoesNotThrow(() -> ctx.sleepFor("wait", 60));
    }

    @Test
    void heartbeat_isNoOp() {
        var ctx = TestTaskContext.builder().build();
        assertDoesNotThrow(() -> ctx.heartbeat());
        assertDoesNotThrow(() -> ctx.heartbeat(30));
    }

    @Test
    void taskID_returnsConfiguredId() {
        var ctx = TestTaskContext.builder().taskId("my-task-123").build();
        assertEquals("my-task-123", ctx.taskID());
    }

    @Test
    void taskID_generatesUuidWhenNotSet() {
        var ctx = TestTaskContext.builder().build();
        assertNotNull(ctx.taskID());
        assertFalse(ctx.taskID().isEmpty());
    }

    @Test
    void headers_returnsConfiguredHeaders() {
        var ctx = TestTaskContext.builder()
                .headers(Map.of("tenant", "acme", "priority", "high"))
                .build();

        assertEquals("acme", ctx.headers().get("tenant"));
        assertEquals("high", ctx.headers().get("priority"));
    }

    @Test
    void headers_emptyByDefault() {
        var ctx = TestTaskContext.builder().build();
        assertTrue(ctx.headers().isEmpty());
    }

    @Test
    void fullHandlerTest_withStepsAndEvents() throws Exception {
        // Simulates testing a real task handler end-to-end
        TaskHandler<Map, Map> handler = (params, ctx) -> {
            var payment = ctx.step("charge", String.class, () -> "pay-" + params.get("orderId"));
            var shipment = ctx.awaitEvent("shipped:" + params.get("orderId"));
            ctx.emitEvent("order.completed", Map.of("payment", payment));
            return Map.of("tracking", shipment.node().get("tracking").asText());
        };

        var ctx = TestTaskContext.builder()
                .eventResponse("shipped:42", Map.of("tracking", "TRACK-99"))
                .build();

        var result = handler.execute(Map.of("orderId", "42"), ctx);

        assertEquals("TRACK-99", result.get("tracking"));
        assertEquals("pay-42", ctx.getStepResults().get("charge").as(String.class));
        assertEquals(1, ctx.getEmittedEvents().size());
    }
}
