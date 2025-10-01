package com.williamcallahan.book_recommendation_engine.quality;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guard test to prevent reintroduction of cache-based cover source enums
 * (LOCAL_CACHE, S3_CACHE) in runtime code. Storage location must be tracked
 * via ImageDetails.storageLocation, not via CoverImageSource or ImageSourceName.
 *
 * Allowed occurrences:
 * - Enum declaration files themselves
 * - Deprecated legacy service: CoverImageService (until removal)
 */
public class CacheSourceEnumsGuardTest {

    private static final Set<String> ALLOWED_PATHS = Set.of(
        normalize("java/com/williamcallahan/book_recommendation_engine/model/image/CoverImageSource.java"),
        normalize("java/com/williamcallahan/book_recommendation_engine/model/image/ImageSourceName.java"),
        normalize("java/com/williamcallahan/book_recommendation_engine/service/image/CoverImageService.java")
    );

    private static final List<String> FORBIDDEN_PATTERNS = List.of(
        "CoverImageSource.LOCAL_CACHE",
        "CoverImageSource.S3_CACHE",
        "ImageSourceName.LOCAL_CACHE",
        "ImageSourceName.S3_CACHE"
    );

    @Test
    void noCacheBasedSourceEnumUsageOutsideEnums() throws IOException {
        Path srcMain = Paths.get(normalize("src/main/java"));
        List<String> violations = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(srcMain)) {
            for (Path path : paths.filter(Files::isRegularFile).collect(Collectors.toList())) {
                String rel = normalize(srcMain.getParent() == null ? path.toString() : srcMain.getParent().relativize(path).toString());
                if (ALLOWED_PATHS.contains(rel)) {
                    continue;
                }
                String content = Files.readString(path, StandardCharsets.UTF_8);
                for (String pattern : FORBIDDEN_PATTERNS) {
                    if (content.contains(pattern)) {
                        violations.add(rel + " contains forbidden pattern: " + pattern);
                    }
                }
            }
        }
        assertTrue(violations.isEmpty(), "Forbidden cache-based source enum usage found:\n" + String.join("\n", violations));
    }

    private static String normalize(String p) {
        return p.replace('\\', '/');
    }
}
