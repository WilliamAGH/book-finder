/**
 * Service for building Redis keys using configured prefixes
 * Provides centralized key generation with external configuration support
 *
 * @author William Callahan
 */
package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.config.RedisKeyProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import java.util.HashSet;
import java.util.Set;

@Service
public class RedisKeyService {
    
    private static final Logger logger = LoggerFactory.getLogger(RedisKeyService.class);
    private final RedisKeyProperties properties;
    
    public RedisKeyService(RedisKeyProperties properties) {
        this.properties = properties;
    }
    
    public String bookKey(String bookId) {
        return properties.getBookPrefix() + bookId;
    }
    
    public String isbn10Key(String isbn) {
        return properties.getIsbn10IndexPrefix() + isbn;
    }
    
    public String isbn13Key(String isbn) {
        return properties.getIsbn13IndexPrefix() + isbn;
    }
    
    public String googleBooksIdKey(String googleBooksId) {
        return properties.getGoogleBooksIdIndexPrefix() + googleBooksId;
    }
    
    public String searchCacheKey(String searchKey) {
        return properties.getSearchCachePrefix() + sanitizeKey(searchKey);
    }
    
    public String bookKeyPattern() {
        return properties.getBookPrefix() + "*";
    }
    
    public String extractIdFromBookKey(String key) {
        if (key != null && key.startsWith(properties.getBookPrefix())) {
            return key.substring(properties.getBookPrefix().length());
        }
        return key;
    }
    
    public String getBookPrefix() {
        return properties.getBookPrefix();
    }
    
    /**
     * Performs Redis SCAN operation to find keys matching a pattern
     * Uses cursor-based iteration for memory-efficient scanning of large key sets
     * 
     * @param jedisPooled Redis connection pool
     * @param pattern key pattern to match (supports wildcards)
     * @param count hint for number of keys to return per scan iteration
     * @return Set of matching keys
     */
    public Set<String> scanKeys(JedisPooled jedisPooled, String pattern, int count) {
        Set<String> keys = new HashSet<>();
        String cursor = "0";
        ScanParams scanParams = new ScanParams().match(pattern).count(count);
        
        try {
            do {
                ScanResult<String> scanResult = jedisPooled.scan(cursor, scanParams);
                cursor = scanResult.getCursor();
                keys.addAll(scanResult.getResult());
            } while (!"0".equals(cursor));
        } catch (Exception e) {
            logger.error("Error during Redis SCAN operation with pattern '{}': {}", pattern, e.getMessage(), e);
        }
        
        return keys;
    }
    
    private String sanitizeKey(String key) {
        if (key == null) return "";
        return key.toLowerCase().trim().replaceAll("\\s+", "_");
    }
}