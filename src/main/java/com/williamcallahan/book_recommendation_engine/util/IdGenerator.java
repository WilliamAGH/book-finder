package com.williamcallahan.book_recommendation_engine.util;

import java.security.SecureRandom;
import java.util.UUID;

/**
 * Minimal ID utilities
 * - NanoId generator (URL-safe)
 * - UUID v7 generator (time-ordered)
 */
public final class IdGenerator {
    // Base62 alphabet: digits + lowercase + uppercase
    private static final char[] DEFAULT_ALPHABET =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final int DEFAULT_SIZE = 10; // Increased for better collision resistance

    // Single SecureRandom instance; thread-safe for concurrent use
    private static final SecureRandom RANDOM = new SecureRandom();

    private IdGenerator() {}

    /** NanoId default 10-char id for most tables */
    public static String generate() {
        return generate(DEFAULT_SIZE);
    }

    /** NanoId 8-char id for low-volume tables */
    public static String generateShort() {
        return generate(8);
    }

    /** NanoId 12-char id for high-volume tables */
    public static String generateLong() {
        return generate(12);
    }

    /** NanoId with size */
    public static String generate(int size) {
        return generate(size, DEFAULT_ALPHABET);
    }

    /** NanoId with custom alphabet */
    public static String generate(int size, char[] alphabet) {
        if (size <= 0) {
            throw new IllegalArgumentException("size must be > 0");
        }
        if (alphabet == null || alphabet.length == 0) {
            throw new IllegalArgumentException("alphabet must not be empty");
        }

        char[] id = new char[size];
        // Draw uniformly from alphabet
        for (int i = 0; i < size; i++) {
            int n = RANDOM.nextInt(alphabet.length);
            id[i] = alphabet[n];
        }
        return new String(id);
    }

    /** Time-ordered epoch UUID v7 string */
    public static String uuidV7() {
        long ts = System.currentTimeMillis() & 0xFFFFFFFFFFFFL; // 48-bit millis
        int randA = RANDOM.nextInt(1 << 12) & 0x0FFF;            // 12-bit random

        long msb = (ts << 16) | (0x7L << 12) | randA;           // 48 ts | ver=7 | randA

        long randB = RANDOM.nextLong();
        long lsb = (randB & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L; // variant 10 + 62-bit rand

        return new UUID(msb, lsb).toString();
    }
}