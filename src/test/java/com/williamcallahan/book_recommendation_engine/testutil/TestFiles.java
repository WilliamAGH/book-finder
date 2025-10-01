package com.williamcallahan.book_recommendation_engine.testutil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Small helpers for temp directories/files in tests. */
public final class TestFiles {
    private TestFiles() {}

    public static Path createTempDir(String prefix) throws IOException {
        return Files.createTempDirectory(prefix);
    }

    public static Path writeBytes(Path dir, String fileName, byte[] data) throws IOException {
        Path f = dir.resolve(fileName);
        Files.write(f, data);
        return f;
    }

    public static void deleteRecursive(Path dir) {
        if (dir == null) return;
        try {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                    });
        } catch (IOException ignored) {}
    }
}