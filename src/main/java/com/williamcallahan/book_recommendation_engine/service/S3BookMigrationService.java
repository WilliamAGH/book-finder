package com.williamcallahan.book_recommendation_engine.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.williamcallahan.book_recommendation_engine.model.Book;
import com.williamcallahan.book_recommendation_engine.service.s3.S3FetchResult;
import com.williamcallahan.book_recommendation_engine.util.BookJsonParser;
import com.williamcallahan.book_recommendation_engine.util.IdGenerator;
import com.williamcallahan.book_recommendation_engine.util.S3Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.ToIntBiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Handles the heavy S3 migration logic extracted from {@link BookDataOrchestrator} so the
 * orchestrator can delegate and remain compact.
 */
class S3BookMigrationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BookMigrationService.class);
    private static final Pattern CONTROL_CHAR_PATTERN = Pattern.compile("[\\x01-\\x08\\x0B\\x0C\\x0E-\\x1F\\x7F]");
    private static final Pattern CONCATENATED_OBJECT_PATTERN = Pattern.compile("\\}\\s*\\{");
    private static final int MAX_JSON_START_SCAN = 1000;
    private static final List<String> JSON_START_HINTS = List.of(
        "{\"id\":",
        "{\"kind\":",
        "{\"title\":",
        "{\"volumeInfo\":",
        "{ \"id\":",
        "{ \"kind\":",
        "[{\""
    );

    private final S3StorageService s3StorageService;
    private final ObjectMapper objectMapper;
    private final BookCollectionPersistenceService collectionPersistenceService;
    private final BiConsumer<Book, JsonNode> persistenceCallback;

    S3BookMigrationService(S3StorageService s3StorageService,
                           ObjectMapper objectMapper,
                           BookCollectionPersistenceService collectionPersistenceService,
                           BiConsumer<Book, JsonNode> persistenceCallback) {
        this.s3StorageService = s3StorageService;
        this.objectMapper = objectMapper;
        this.collectionPersistenceService = collectionPersistenceService;
        this.persistenceCallback = persistenceCallback;
    }

    void migrateBooksFromS3(String prefix, int maxRecords, int skipRecords) {
        if (s3StorageService == null) {
            LOGGER.warn("S3→DB migration skipped: S3 is not configured (S3StorageService is null).");
            return;
        }

        final String effectivePrefix = S3Paths.ensureTrailingSlash(prefix);
        LOGGER.info("Starting S3→DB migration: prefix='{}', max={}, skip={}", effectivePrefix, maxRecords, skipRecords);

        List<String> keysToProcess = prepareJsonKeysForMigration(
            effectivePrefix,
            maxRecords,
            skipRecords,
            "S3→DB migration",
            " Nothing to do.",
            " after filtering."
        );

        if (keysToProcess.isEmpty()) {
            return;
        }

        processS3JsonKeys(
            keysToProcess,
            "S3→DB migration",
            50,
            -1,
            (key, rawJson) -> {
                List<JsonNode> candidates = parseBookJsonPayload(rawJson, key);
                if (candidates.isEmpty()) {
                    LOGGER.debug("S3→DB migration: No usable JSON objects extracted for key {}.", key);
                    return 0;
                }

                int processedCount = 0;
                for (JsonNode jsonNode : candidates) {
                    Book book = BookJsonParser.convertJsonToBook(jsonNode);
                    if (book == null || book.getId() == null) {
                        LOGGER.debug("S3→DB migration: Parsed null/invalid book for key {}. Skipping fragment.", key);
                        continue;
                    }

                    persistenceCallback.accept(book, jsonNode);
                    processedCount++;
                }
                return processedCount;
            }
        );
    }

    void migrateListsFromS3(String provider, String prefix, int maxRecords, int skipRecords) {
        if (collectionPersistenceService == null || s3StorageService == null) {
            LOGGER.warn("S3→DB list migration skipped: Required services are not configured.");
            return;
        }
        String defaultPrefix = "lists/" + (provider != null ? provider.toLowerCase(Locale.ROOT) : "") + "/";
        final String effectivePrefix = S3Paths.ensureTrailingSlash(prefix, defaultPrefix);
        LOGGER.info("Starting S3→DB list migration: provider='{}', prefix='{}', max={}, skip={}", provider, effectivePrefix, maxRecords, skipRecords);

        List<String> keysToProcess = prepareJsonKeysForMigration(
            effectivePrefix,
            maxRecords,
            skipRecords,
            "S3→DB list migration",
            "",
            "."
        );

        if (keysToProcess.isEmpty()) {
            return;
        }

        int totalToProcess = keysToProcess.size();

        processS3JsonKeys(
            keysToProcess,
            "S3→DB list migration",
            20,
            totalToProcess,
            (key, rawJson) -> {
                try {
                    JsonNode root = objectMapper.readTree(rawJson);
                    if (!(root instanceof com.fasterxml.jackson.databind.node.ObjectNode listJson)) {
                        return 0;
                    }

                    String displayName = listJson.path("display_name").asText(null);
                    String listNameEncoded = listJson.path("list_name_encoded").asText(null);
                    String updatedFrequency = listJson.path("updated_frequency").asText(null);
                    java.time.LocalDate bestsellersDate = null;
                    java.time.LocalDate publishedDate = null;
                    try { String s = listJson.path("bestsellers_date").asText(null); if (s != null) bestsellersDate = java.time.LocalDate.parse(s); } catch (Exception ignored) {}
                    try { String s = listJson.path("published_date").asText(null); if (s != null) publishedDate = java.time.LocalDate.parse(s); } catch (Exception ignored) {}
                    if (publishedDate == null || listNameEncoded == null || provider == null) {
                        LOGGER.warn("Skipping list key {}: missing required fields provider/list_name_encoded/published_date.", key);
                        return 0;
                    }

                    String listId = collectionPersistenceService
                        .upsertList(provider, listNameEncoded, publishedDate, displayName, bestsellersDate, updatedFrequency, null, listJson)
                        .orElse(null);
                    if (listId == null) {
                        return 0;
                    }

                    JsonNode booksArray = listJson.path("books");
                    if (booksArray != null && booksArray.isArray()) {
                        for (JsonNode item : booksArray) {
                            Integer rank = item.path("rank").isNumber() ? item.path("rank").asInt() : null;
                            Integer weeksOnList = item.path("weeks_on_list").isNumber() ? item.path("weeks_on_list").asInt() : null;
                            String isbn13 = item.path("primary_isbn13").asText(null);
                            String isbn10 = item.path("primary_isbn10").asText(null);
                            String providerRef = item.path("amazon_product_url").asText(null);

                            Book minimal = new Book();
                            minimal.setId(isbn13 != null ? isbn13 : (isbn10 != null ? isbn10 : IdGenerator.uuidV7()));
                            minimal.setIsbn13(isbn13);
                            minimal.setIsbn10(isbn10);
                            minimal.setTitle(item.path("title").asText(null));
                            minimal.setPublisher(item.path("publisher").asText(null));
                            minimal.setExternalImageUrl(item.path("book_image").asText(null));

                            persistenceCallback.accept(minimal, null);
                            String canonicalId = minimal.getId();

                            collectionPersistenceService.upsertListMembership(listId, canonicalId, rank, weeksOnList, isbn13, isbn10, providerRef, item);
                        }
                    }

                    return 1;
                } catch (Exception ex) {
                    LOGGER.warn("S3→DB list migration: Error processing key {}: {}", key, ex.getMessage());
                    return 0;
                }
            }
        );
    }

    private List<String> prepareJsonKeysForMigration(String effectivePrefix,
                                                    int maxRecords,
                                                    int skipRecords,
                                                    String contextLabel,
                                                    String noObjectsSuffix,
                                                    String totalSuffix) {
        List<S3Object> objects = s3StorageService.listObjects(effectivePrefix);
        if (objects == null || objects.isEmpty()) {
            LOGGER.info("{}: No objects found under prefix '{}'{}", contextLabel, effectivePrefix, noObjectsSuffix);
            return Collections.emptyList();
        }

        List<String> jsonKeys = new ArrayList<>();
        for (S3Object object : objects) {
            if (object == null) {
                continue;
            }
            String key = object.key();
            if (key != null && key.endsWith(".json")) {
                jsonKeys.add(key);
            }
        }

        if (skipRecords > 0 && skipRecords < jsonKeys.size()) {
            jsonKeys = new ArrayList<>(jsonKeys.subList(skipRecords, jsonKeys.size()));
        } else if (skipRecords >= jsonKeys.size()) {
            LOGGER.info("{}: skip={} >= total JSON objects ({}). Nothing to do.", contextLabel, skipRecords, jsonKeys.size());
            return Collections.emptyList();
        }

        int totalToProcess = (maxRecords > 0) ? Math.min(maxRecords, jsonKeys.size()) : jsonKeys.size();
        LOGGER.info("{}: {} JSON object(s) to process{}", contextLabel, totalToProcess, totalSuffix);
        return new ArrayList<>(jsonKeys.subList(0, totalToProcess));
    }

    private List<JsonNode> parseBookJsonPayload(String rawPayload, String key) {
        if (rawPayload == null || rawPayload.isBlank()) {
            LOGGER.warn("S3→DB migration: Empty JSON payload for key {}.", key);
            return Collections.emptyList();
        }

        String sanitized = rawPayload.replace("\u0000", "");
        sanitized = CONTROL_CHAR_PATTERN.matcher(sanitized).replaceAll("");
        sanitized = sanitized.trim();

        if (sanitized.isEmpty()) {
            LOGGER.warn("S3→DB migration: Payload for key {} became empty after sanitization.", key);
            return Collections.emptyList();
        }

        if (!sanitized.startsWith("{") && !sanitized.startsWith("[")) {
            int startIndex = findJsonStartIndex(sanitized);
            if (startIndex < 0) {
                LOGGER.warn("S3→DB migration: Unable to locate JSON start for key {}. Skipping payload.", key);
                return Collections.emptyList();
            }
            if (startIndex > 0) {
                LOGGER.warn("S3→DB migration: Stripping {} leading non-JSON bytes for key {}.", startIndex, key);
                sanitized = sanitized.substring(startIndex);
            }
        }

        List<String> fragments = splitConcatenatedJson(sanitized);
        List<JsonNode> parsedNodes = new ArrayList<>();

        for (String fragment : fragments) {
            if (fragment == null || fragment.isBlank()) {
                continue;
            }
            try {
                JsonNode node = objectMapper.readTree(fragment);
                if (node == null) {
                    continue;
                }
                if (node.isArray()) {
                    node.forEach(parsedNodes::add);
                } else {
                    parsedNodes.add(node);
                }
            } catch (Exception ex) {
                LOGGER.warn("S3→DB migration: Failed to parse JSON fragment for key {}: {}", key, ex.getMessage());
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Fragment preview: {}", fragment.length() > 200 ? fragment.substring(0, 200) : fragment);
                }
            }
        }

        if (parsedNodes.isEmpty()) {
            return Collections.emptyList();
        }

        List<JsonNode> extracted = new ArrayList<>(parsedNodes.size());
        for (JsonNode candidate : parsedNodes) {
            JsonNode normalized = extractFromPreProcessed(candidate, key);
            if (normalized != null) {
                extracted.add(normalized);
            }
        }

        return deduplicateBookNodes(extracted, key);
    }

    private int findJsonStartIndex(String payload) {
        if (payload == null) {
            return -1;
        }

        int firstBrace = payload.indexOf('{');
        int firstBracket = payload.indexOf('[');
        int candidate = -1;

        if (firstBrace >= 0 && (firstBracket == -1 || firstBrace < firstBracket)) {
            candidate = firstBrace;
        } else if (firstBracket >= 0) {
            candidate = firstBracket;
        }

        if (candidate >= 0 && candidate <= MAX_JSON_START_SCAN) {
            return candidate;
        }

        int bestMatch = -1;
        for (String hint : JSON_START_HINTS) {
            int idx = payload.indexOf(hint);
            if (idx >= 0 && (bestMatch == -1 || idx < bestMatch)) {
                bestMatch = idx;
            }
        }

        return bestMatch;
    }

    private List<String> splitConcatenatedJson(String payload) {
        Matcher matcher = CONCATENATED_OBJECT_PATTERN.matcher(payload);
        if (!matcher.find()) {
            return List.of(payload);
        }

        List<String> fragments = new ArrayList<>();
        int depth = 0;
        int start = 0;

        for (int i = 0; i < payload.length(); i++) {
            char ch = payload.charAt(i);
            if (ch == '{') {
                depth++;
            } else if (ch == '}') {
                depth--;
                if (depth == 0) {
                    fragments.add(payload.substring(start, i + 1));
                    start = i + 1;
                    while (start < payload.length() && Character.isWhitespace(payload.charAt(start))) {
                        start++;
                    }
                }
            }
        }

        if (start < payload.length()) {
            String remainder = payload.substring(start).trim();
            if (!remainder.isEmpty()) {
                fragments.add(remainder);
            }
        }

        return fragments.isEmpty() ? List.of(payload) : fragments;
    }

    private JsonNode extractFromPreProcessed(JsonNode node, String key) {
        if (node == null) {
            return null;
        }

        JsonNode rawNode = node.get("rawJsonResponse");
        if (rawNode == null) {
            return node;
        }
        boolean hasVolumeInfo = node.has("volumeInfo");
        String id = node.path("id").asText(null);
        String title = node.path("title").asText(null);

        boolean isPreProcessed = rawNode != null && !hasVolumeInfo && id != null && id.equals(title);
        if (!isPreProcessed) {
            return node;
        }

        try {
            String raw = rawNode.isTextual() ? rawNode.asText() : rawNode.toString();
            if (raw.startsWith("\"") && raw.endsWith("\"")) {
                raw = objectMapper.readValue(raw, String.class);
            }
            JsonNode inner = objectMapper.readTree(raw);
            if (inner != null && (inner.has("volumeInfo") || "books#volume".equals(inner.path("kind").asText(null)))) {
                return inner;
            }
        } catch (Exception ex) {
            LOGGER.warn("S3→DB migration: Failed to unwrap rawJsonResponse for key {}: {}", key, ex.getMessage());
        }

        return node;
    }

    private List<JsonNode> deduplicateBookNodes(List<JsonNode> candidates, String key) {
        if (candidates == null || candidates.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, JsonNode> unique = new LinkedHashMap<>();
        for (JsonNode candidate : candidates) {
            if (candidate == null) {
                continue;
            }
            String dedupKey = computeDedupKey(candidate);
            if (!unique.containsKey(dedupKey)) {
                unique.put(dedupKey, candidate);
            } else if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("S3→DB migration: Deduplicated book fragment for key {} using key {}.", key, dedupKey);
            }
        }

        return new ArrayList<>(unique.values());
    }

    private String computeDedupKey(JsonNode node) {
        if (node == null) {
            return "";
        }

        String nodeId = node.path("id").asText(null);

        JsonNode volume = node.has("volumeInfo") ? node.get("volumeInfo") : node;
        JsonNode identifiers = volume.get("industryIdentifiers");
        if (identifiers != null && identifiers.isArray()) {
            for (JsonNode identifierNode : identifiers) {
                String type = identifierNode.path("type").asText("");
                String value = identifierNode.path("identifier").asText("");
                if (!value.isEmpty() && ("ISBN_13".equalsIgnoreCase(type) || "ISBN_10".equalsIgnoreCase(type))) {
                    return (type + ":" + value).toLowerCase(Locale.ROOT);
                }
            }
        }

        if (nodeId != null && !nodeId.isBlank()) {
            return ("id:" + nodeId).toLowerCase(Locale.ROOT);
        }

        String title = volume.path("title").asText("");
        String author = "";
        JsonNode authors = volume.get("authors");
        if (authors != null && authors.isArray() && authors.size() > 0) {
            author = authors.get(0).asText("");
        }

        return (title + ":" + author).toLowerCase(Locale.ROOT);
    }

    private Optional<String> fetchJsonFromS3Key(String key) {
        if (s3StorageService == null) {
            return Optional.empty();
        }

        try {
            S3FetchResult<String> result = s3StorageService.fetchGenericJsonAsync(key).join();
            if (result.isSuccess()) {
                return result.getData();
            }

            if (result.isNotFound()) {
                LOGGER.debug("S3→DB migration: JSON not found for key {}. Skipping.", key);
            } else if (result.isDisabled()) {
                LOGGER.warn("S3→DB migration: S3 disabled while fetching key {}. Skipping.");
            } else {
                String errorMessage = result.getErrorMessage().orElse("Unknown error");
                LOGGER.warn("S3→DB migration: Failed to fetch key {} (status {}): {}", key, result.getStatus(), errorMessage);
            }
        } catch (CompletionException ex) {
            Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
            LOGGER.warn("S3→DB migration: Exception fetching key {}: {}", key, cause.getMessage());
        }

        return Optional.empty();
    }

    private void processS3JsonKeys(List<String> keys,
                                   String contextLabel,
                                   int progressInterval,
                                   int expectedTotal,
                                   ToIntBiFunction<String, String> handler) {
        if (keys == null || keys.isEmpty()) {
            LOGGER.info("{}: No S3 objects to process.", contextLabel);
            return;
        }

        AtomicInteger processed = new AtomicInteger(0);

        for (String key : keys) {
            Optional<String> jsonOptional = fetchJsonFromS3Key(key);
            if (jsonOptional.isEmpty()) {
                continue;
            }

            try {
                int processedForKey = handler.applyAsInt(key, jsonOptional.get());
                if (processedForKey <= 0) {
                    continue;
                }

                int totalProcessed = processed.addAndGet(processedForKey);
                if (progressInterval > 0 && (totalProcessed % progressInterval == 0 || (expectedTotal > 0 && totalProcessed >= expectedTotal))) {
                    if (expectedTotal > 0) {
                        LOGGER.info("{} progress: {}/{} processed.", contextLabel, Math.min(totalProcessed, expectedTotal), expectedTotal);
                    } else {
                        LOGGER.info("{} progress: {} processed so far (latest key: {}).", contextLabel, totalProcessed, key);
                    }
                }
            } catch (Exception ex) {
                LOGGER.warn("{}: Error processing key {}: {}", contextLabel, key, ex.getMessage());
            }
        }

        if (expectedTotal > 0) {
            LOGGER.info("{} completed. Processed {}/{} record(s).", contextLabel, Math.min(processed.get(), expectedTotal), expectedTotal);
        } else {
            LOGGER.info("{} completed. Processed {} record(s).", contextLabel, processed.get());
        }
    }
}
