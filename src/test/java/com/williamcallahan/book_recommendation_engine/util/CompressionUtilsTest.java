package com.williamcallahan.book_recommendation_engine.util;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompressionUtilsTest {

    @Test
    void decodeUtf8WithOptionalGzip_handlesPlainUtf8() {
        String payload = "plain-text";
        String decoded = CompressionUtils.decodeUtf8WithOptionalGzip(payload.getBytes(StandardCharsets.UTF_8));
        assertThat(decoded).isEqualTo(payload);
    }

    @Test
    void decodeUtf8WithOptionalGzip_handlesGzipPayload() throws IOException {
        byte[] gzipped = gzip("compressed-json");
        String decoded = CompressionUtils.decodeUtf8WithOptionalGzip(gzipped);
        assertThat(decoded).isEqualTo("compressed-json");
    }

    @Test
    void decodeUtf8ExpectingGzip_returnsDecodedString() throws IOException {
        byte[] gzipped = gzip("expect-gzip");
        String decoded = CompressionUtils.decodeUtf8ExpectingGzip(gzipped);
        assertThat(decoded).isEqualTo("expect-gzip");
    }

    @Test
    void decodeUtf8ExpectingGzip_throwsForPlainBytes() {
        byte[] payload = "not-gzip".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> CompressionUtils.decodeUtf8ExpectingGzip(payload))
                .isInstanceOf(IOException.class);
    }

    private byte[] gzip(String value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(value.getBytes(StandardCharsets.UTF_8));
        }
        return baos.toByteArray();
    }
}
