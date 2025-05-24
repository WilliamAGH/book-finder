/**
 * Service for interacting with Redis JSON module capabilities
 *
 * @author William Callahan
 *
 * Features:
 * - Provides JSON storage and retrieval operations using Redis
 * - Supports setting and getting JSON at specific paths
 * - Includes Redis server health check via ping
 * - Uses Jedis client for Redis communication
 * - Designed for integration with S3-to-Redis migration process
 */
package com.williamcallahan.book_recommendation_engine.jsontoredis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2; // Using non-deprecated Path2
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

@Service("jsonS3ToRedis_RedisJsonService")
@Profile("jsontoredis")
public class RedisJsonService {

    private final JedisPooled jedis;
    private final ObjectMapper objectMapper; // Added ObjectMapper
    private static final Logger log = LoggerFactory.getLogger(RedisJsonService.class);

    public RedisJsonService(@Qualifier("jsonS3ToRedisJedisPooled") JedisPooled jedis, ObjectMapper objectMapper) { // Changed to inject JedisPooled and ObjectMapper
        this.jedis = jedis;
        this.objectMapper = objectMapper; // Initialize ObjectMapper
    }

    /**
     * Stores a JSON string at a given key and path
     * @param key The Redis key
     * @param pathString The JSON path string (use "$" or "." for root)
     * @param jsonString The JSON string value to set
     */
    public void jsonSet(String key, String pathString, String jsonString) {
        try {
            Path2 path = Path2.of(pathString);
            // Parse the jsonString into an Object (e.g., Map or List)
            // so Jedis serializes it as a JSON structure, not a JSON string literal
            Object jsonObject = objectMapper.readValue(jsonString, Object.class);
            jedis.jsonSet(key, path, jsonObject);
            log.debug("Set JSON for key {} at path {}", key, pathString);
        } catch (JsonProcessingException e) {
            log.error("Error parsing jsonString before setting JSON for key {} at path {}: {}", key, pathString, e.getMessage(), e);
            // Handle or rethrow as appropriate
        } catch (Exception e) {
            log.error("Error setting JSON for key {} at path {}: {}", key, pathString, e.getMessage(), e);
            // Consider rethrowing a custom exception or a JedisException if callers need to react
        }
    }

    /**
     * Gets a JSON value from a given key and path
     * @param key The Redis key
     * @param pathString The JSON path string
     * @return A string representing the JSON result. Returns null if key/path not found or error
     */
    public String jsonGet(String key, String pathString) {
        log.debug("Getting JSON for key {} at path {}", key, pathString);
        try {
            Path2 path = Path2.of(pathString);
            // Attempting to get the result as a generic Object first.
            // The actual return type might depend on the JSON structure.
            // If a specific type is expected (e.g. String, Map), jsonGetAs() might be more appropriate.
            Object result = jedis.jsonGet(key, path); 
            if (result == null) {
                log.debug("No JSON found for key {} at path {}", key, pathString);
                return null;
            }
            // Convert to string. For complex objects, this will be the default toString(),
            // which might not be the JSON string representation.
            // Serialize the result object back to a JSON string if it's not null.
            if (result instanceof String) { // If RedisJSON already returned it as a simple string (e.g. a value at a deeper path like a string field)
                return (String) result;
            }
            return objectMapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            log.error("Error serializing result to JSON for key {} at path {}: {}", key, pathString, e.getMessage(), e);
            return null;
        } catch (Exception e) {
            log.warn("Error getting JSON for key {} at path {}: {}", key, pathString, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Checks if a key exists in Redis
     * 
     * @param key The Redis key to check
     * @return true if the key exists, false otherwise
     */
    public boolean keyExists(String key) {
        log.debug("Checking if key {} exists", key);
        try {
            return jedis.exists(key);
        } catch (Exception e) {
            log.error("Error checking existence of key {}: {}", key, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Performs a PING to the Redis server
     * @return The server's response to PING, typically "PONG"
     */
    public String ping() {
        try {
            return jedis.ping();
        } catch (Exception e) {
            log.error("Error pinging Redis: {}", e.getMessage(), e);
            throw e; // Rethrow the exception to be handled by the caller
        }
    }
}
