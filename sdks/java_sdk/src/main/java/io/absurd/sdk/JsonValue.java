package io.absurd.sdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;

/**
 * Wraps a Jackson JsonNode to represent arbitrary JSON values flowing through Absurd.
 */
public final class JsonValue {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonValue NULL = new JsonValue(NullNode.getInstance());

    private final JsonNode node;

    private JsonValue(JsonNode node) {
        this.node = node != null ? node : NullNode.getInstance();
    }

    public static JsonValue of(JsonNode node) {
        return new JsonValue(node);
    }

    public static JsonValue ofNull() {
        return NULL;
    }

    public static JsonValue fromObject(Object value) {
        if (value == null) {
            return NULL;
        }
        return new JsonValue(MAPPER.valueToTree(value));
    }

    public static JsonValue parse(String json) {
        if (json == null) {
            return NULL;
        }
        try {
            return new JsonValue(MAPPER.readTree(json));
        } catch (JsonProcessingException e) {
            throw new AbsurdException("Failed to parse JSON: " + e.getMessage(), e);
        }
    }

    @com.fasterxml.jackson.annotation.JsonValue
    public JsonNode node() {
        return node;
    }

    public <T> T as(Class<T> type) {
        try {
            return MAPPER.treeToValue(node, type);
        } catch (JsonProcessingException e) {
            throw new AbsurdException("Failed to convert JSON to " + type.getName(), e);
        }
    }

    public String toJson() {
        try {
            return MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new AbsurdException("Failed to serialize JSON", e);
        }
    }

    public boolean isNull() {
        return node.isNull();
    }

    @Override
    public String toString() {
        return toJson();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JsonValue other)) return false;
        return node.equals(other.node);
    }

    @Override
    public int hashCode() {
        return node.hashCode();
    }
}
