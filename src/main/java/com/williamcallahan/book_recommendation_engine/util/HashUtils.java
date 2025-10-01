package com.williamcallahan.book_recommendation_engine.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Generic hashing utilities for data integrity and deduplication.
 * 
 * Provides centralized SHA-256 hashing operations with consistent error handling
 * and multiple output formats (byte array, hex string, base64).
 * 
 * @author William Callahan
 * @since 0.9.0
 */
public final class HashUtils {
    
    private HashUtils() {
        // Utility class - prevent instantiation
    }
    
    /**
     * Computes SHA-256 hash of byte array data.
     * 
     * @param data Byte array to hash
     * @return SHA-256 hash as byte array
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @example
     * <pre>{@code
     * byte[] imageData = loadImage();
     * byte[] hash = HashUtils.computeSha256(imageData);
     * }</pre>
     */
    public static byte[] computeSha256(byte[] data) throws NoSuchAlgorithmException {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return digest.digest(data);
    }
    
    /**
     * Computes SHA-256 hash of string data using UTF-8 encoding.
     * 
     * @param data String to hash
     * @return SHA-256 hash as byte array
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @example
     * <pre>{@code
     * String url = "https://example.com/image.jpg";
     * byte[] hash = HashUtils.computeSha256(url);
     * }</pre>
     */
    public static byte[] computeSha256(String data) throws NoSuchAlgorithmException {
        if (data == null) {
            throw new IllegalArgumentException("Data cannot be null");
        }
        return computeSha256(data.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Computes SHA-256 hash and returns as hexadecimal string.
     * 
     * @param data Byte array to hash
     * @return SHA-256 hash as lowercase hex string (64 characters)
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @example
     * <pre>{@code
     * byte[] data = loadData();
     * String hex = HashUtils.sha256Hex(data);
     * // Returns: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
     * }</pre>
     */
    public static String sha256Hex(byte[] data) throws NoSuchAlgorithmException {
        byte[] hash = computeSha256(data);
        return bytesToHex(hash);
    }
    
    /**
     * Computes SHA-256 hash of string and returns as hexadecimal string.
     * 
     * @param data String to hash
     * @return SHA-256 hash as lowercase hex string (64 characters)
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @example
     * <pre>{@code
     * String text = "Hello, World!";
     * String hex = HashUtils.sha256Hex(text);
     * }</pre>
     */
    public static String sha256Hex(String data) throws NoSuchAlgorithmException {
        return sha256Hex(data.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Computes SHA-256 hash and returns as URL-safe Base64 string.
     * 
     * @param data Byte array to hash
     * @return SHA-256 hash as URL-safe Base64 string (43 characters without padding)
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @example
     * <pre>{@code
     * byte[] data = loadData();
     * String base64 = HashUtils.sha256Base64(data);
     * // Returns: "47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU"
     * }</pre>
     */
    public static String sha256Base64(byte[] data) throws NoSuchAlgorithmException {
        byte[] hash = computeSha256(data);
        return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    }
    
    /**
     * Computes SHA-256 hash of string and returns as URL-safe Base64 string.
     * 
     * @param data String to hash
     * @return SHA-256 hash as URL-safe Base64 string (43 characters without padding)
     * @throws NoSuchAlgorithmException If SHA-256 algorithm is not available
     * 
     * @example
     * <pre>{@code
     * String url = "https://example.com/image.jpg";
     * String base64 = HashUtils.sha256Base64(url);
     * }</pre>
     */
    public static String sha256Base64(String data) throws NoSuchAlgorithmException {
        return sha256Base64(data.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Converts byte array to lowercase hexadecimal string.
     * 
     * @param bytes Byte array to convert
     * @return Hexadecimal string representation
     */
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
    
    /**
     * Compares two byte arrays for equality in constant time.
     * <p>
     * This method is timing-attack resistant by always comparing
     * all bytes regardless of early mismatches.
     * 
     * @param hash1 First hash
     * @param hash2 Second hash
     * @return true if hashes are equal, false otherwise
     * 
     * @example
     * <pre>{@code
     * byte[] hash1 = HashUtils.computeSha256(data1);
     * byte[] hash2 = HashUtils.computeSha256(data2);
     * boolean equal = HashUtils.constantTimeEquals(hash1, hash2);
     * }</pre>
     */
    public static boolean constantTimeEquals(byte[] hash1, byte[] hash2) {
        if (hash1 == null || hash2 == null) {
            return hash1 == hash2;
        }
        if (hash1.length != hash2.length) {
            return false;
        }
        
        int diff = 0;
        for (int i = 0; i < hash1.length; i++) {
            diff |= hash1[i] ^ hash2[i];
        }
        return diff == 0;
    }
}
