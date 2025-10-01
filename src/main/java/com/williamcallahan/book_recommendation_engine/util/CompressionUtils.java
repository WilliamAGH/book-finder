package com.williamcallahan.book_recommendation_engine.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

/**
 * Utility helpers for decoding byte arrays that may be gzip-compressed JSON.
 */
public final class CompressionUtils {

    private CompressionUtils() {
    }

    /**
     * Attempts to decode the provided bytes by first treating them as gzip-compressed UTF-8.
     * If decompression fails, falls back to interpreting the bytes directly as UTF-8.
     *
     * @param raw byte array to decode
     * @return decoded UTF-8 string, or {@code null} when the payload cannot be interpreted
     */
    public static String decodeUtf8WithOptionalGzip(byte[] raw) {
        if (raw == null || raw.length == 0) {
            return null;
        }
        try {
            return decompressGzip(raw);
        } catch (IOException ignored) {
            // Not gzipped (or failed to decompress); fall back to plain UTF-8.
            try {
                return new String(raw, StandardCharsets.UTF_8);
            } catch (Exception e) {
                return null;
            }
        }
    }

    /**
     * Decodes the provided bytes as gzip-compressed UTF-8. Throws when decompression fails.
     *
     * @param raw byte array expected to contain gzip data
     * @return decoded UTF-8 string, or {@code null} when the payload is empty
     * @throws IOException when decompression fails
     */
    public static String decodeUtf8ExpectingGzip(byte[] raw) throws IOException {
        if (raw == null || raw.length == 0) {
            return null;
        }
        return decompressGzip(raw);
    }

    private static String decompressGzip(byte[] raw) throws IOException {
        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(raw));
             ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            return baos.toString(StandardCharsets.UTF_8);
        } catch (ZipException ex) {
            // Propagate as IOException so callers can decide whether to fallback or fail.
            throw new IOException("Failed to decompress gzip data", ex);
        }
    }
}
