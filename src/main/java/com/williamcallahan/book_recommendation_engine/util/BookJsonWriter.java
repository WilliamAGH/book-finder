package com.williamcallahan.book_recommendation_engine.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.williamcallahan.book_recommendation_engine.model.Book;

import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Shared helpers for serialising {@link Book} objects back to JSON and merging with
 * existing stored payloads. Complements {@link BookJsonParser} so ingest/export paths
 * share a single conversion layer.
 */
public final class BookJsonWriter {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private BookJsonWriter() {
    }

    public static ObjectNode toObjectNode(Book book) {
        if (book == null) {
            return OBJECT_MAPPER.createObjectNode();
        }
        JsonNode node = OBJECT_MAPPER.valueToTree(book);
        if (node instanceof ObjectNode objectNode) {
            return objectNode;
        }
        return OBJECT_MAPPER.createObjectNode();
    }

    public static String toJsonString(Book book) {
        return writeNode(toObjectNode(book));
    }

    public static ObjectNode readObjectNode(String json) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            JsonNode node = OBJECT_MAPPER.readTree(json);
            return node instanceof ObjectNode objectNode ? objectNode : null;
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to read book JSON payload", e);
        }
    }

    public static String mergeBookJson(String existingJson, Book book) {
        ObjectNode bookNode = toObjectNode(book);
        ObjectNode existingNode = readObjectNode(existingJson);
        if (existingNode == null) {
            return writeNode(bookNode);
        }
        mergeQualifiers(existingNode, bookNode);
        return writeNode(existingNode);
    }

    private static void mergeQualifiers(ObjectNode target, ObjectNode source) {
        if (source == null || !source.has("qualifiers")) {
            return;
        }
        JsonNode sourceQualifiers = source.get("qualifiers");
        if (!(sourceQualifiers instanceof ObjectNode sourceObject)) {
            target.set("qualifiers", sourceQualifiers);
            return;
        }

        ObjectNode result;
        if (target.has("qualifiers") && target.get("qualifiers") instanceof ObjectNode targetObject) {
            result = targetObject;
        } else {
            result = OBJECT_MAPPER.createObjectNode();
        }

        Iterator<Map.Entry<String, JsonNode>> fields = sourceObject.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            result.set(entry.getKey(), entry.getValue());
        }
        target.set("qualifiers", result);
    }

    private static String writeNode(ObjectNode node) {
        try {
            return OBJECT_MAPPER.writeValueAsString(node);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException("Failed to serialise book JSON payload", e);
        }
    }
}

