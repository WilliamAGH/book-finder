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
import redis.clients.jedis.json.Path2; // Using new Path2 to replace deprecated Path
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import com.williamcallahan.book_recommendation_engine.util.RedisHelper;

@Service("jsonS3ToRedis_RedisJsonService")
@Profile("jsontoredis")
public class RedisJsonService {

    private final JedisPooled jedis;
    private final ObjectMapper objectMapper;
    private static final Logger log = LoggerFactory.getLogger(RedisJsonService.class);

    public RedisJsonService(@Qualifier("jsonS3ToRedisJedisPooled") JedisPooled jedis, ObjectMapper objectMapper) {
        this.jedis = jedis;
        this.objectMapper = objectMapper;
    }

    /**
     * Stores a JSON string at a given key and path
     * @param key The Redis key
     * @param pathString The JSON path string (use "$" or "." for root)
     * @param jsonString The JSON string value to set
     * @return true if successful, false otherwise
     */
    public boolean jsonSet(String key, String pathString, String jsonString) {
        // Validate JSON before storing
        try {
            objectMapper.readTree(jsonString);
        } catch (JsonProcessingException e) {
            log.error("Invalid JSON for key {} at path {}: {}", key, pathString, e.getMessage(), e);
            return false;
        }
        // Prepare operation data to avoid checked exceptions in the lambda
        boolean isRoot = "$".equals(pathString) || ".".equals(pathString);
        final Object jsonObject;
        if (isRoot) {
            jsonObject = null;
        } else {
            try {
                jsonObject = objectMapper.readValue(jsonString, Object.class);
            } catch (JsonProcessingException e) {
                log.error("Invalid JSON for nested path set for key {} at path {}: {}", key, pathString, e.getMessage(), e);
                return false;
            }
        }
        // Execute the Redis set with timing and circuit breaker
        return RedisHelper.executeWithTiming(
            log,
            () -> {
                if (isRoot) {
                    jedis.jsonSet(key, jsonString);
                    log.debug("Set JSON for key {} at root path", key);
                } else {
                    Path2 path = Path2.of(pathString);
                    jedis.jsonSet(key, path, jsonObject);
                    log.debug("Set JSON for key {} at path {}", key, pathString);
                }
                return true;
            },
            "jsonSet(" + key + "," + pathString + ")",
            false
        );
    }

    /**
     * Gets a JSON value from a given key and path
     * @param key The Redis key
     * @param pathString The JSON path string
     * @return A string representing the JSON result - returns null if key/path not found or error
     */
    public String jsonGet(String key, String pathString) {
        String operationName = "jsonGet(" + key + "," + pathString + ")";
        return RedisHelper.executeWithTiming(
            log,
            () -> {
                Path2 path = Path2.of(pathString);
                Object result = jedis.jsonGet(key, path);
                if (result == null) {
                    return null;
                }
                if (result instanceof String) {
                    return (String) result;
                }
                try {
                    return objectMapper.writeValueAsString(result);
                } catch (JsonProcessingException e) {
                    log.error("Error serializing result to JSON for key {} at path {}: {}", key, pathString, e.getMessage(), e);
                    return null;
                }
            },
            operationName,
            null
        );
    }

    /**
     * Checks if a key exists in Redis
     * 
     * @param key The Redis key to check
     * @return true if the key exists, false otherwise
     */
    public boolean keyExists(String key) {
        // Execute exists with timing and circuit breaker
        return RedisHelper.executeWithTiming(
            log,
            () -> jedis.exists(key),
            "keyExists(" + key + ")",
            false
        );
    }

    /**
     * Performs a PING to the Redis server
     * @return The server's response to PING, typically "PONG"
     */
    public String ping() {
        String operationName = "ping()";
        return RedisHelper.executeWithTiming(
            log,
            () -> jedis.ping(),
            operationName,
            null
        );
    }
    
    /**
     * Gets raw JSON using legacy path to avoid array wrapping
     * This is useful for diagnostics and DataGrip compatibility
     * @param key The Redis key
     * @return The raw JSON string without array wrapping
     */
    public String jsonGetRaw(String key) {
        String operationName = "jsonGetRaw(" + key + ")";
        return RedisHelper.executeWithTiming(
            log,
            () -> {
                Object result = jedis.jsonGet(key);
                return result != null ? result.toString() : null;
            },
            operationName,
            null
        );
    }
}
