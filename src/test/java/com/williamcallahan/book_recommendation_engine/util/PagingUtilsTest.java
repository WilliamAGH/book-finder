package com.williamcallahan.book_recommendation_engine.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PagingUtilsTest {

    @Test
    @DisplayName("clamp constrains values to inclusive range")
    void clampKeepsValuesInRange() {
        assertThat(PagingUtils.clamp(10, 1, 20)).isEqualTo(10);
        assertThat(PagingUtils.clamp(-5, 1, 20)).isEqualTo(1);
        assertThat(PagingUtils.clamp(30, 1, 20)).isEqualTo(20);
    }

    @Test
    @DisplayName("atLeast bumps values below the floor")
    void atLeastRaisesFloor() {
        assertThat(PagingUtils.atLeast(-1, 0)).isEqualTo(0);
        assertThat(PagingUtils.atLeast(5, 1)).isEqualTo(5);
    }

    @Test
    @DisplayName("safeLimit honours defaults and clamps to bounds")
    void safeLimitClampsWithDefault() {
        assertThat(PagingUtils.safeLimit(0, 10, 1, 50)).isEqualTo(10);
        assertThat(PagingUtils.safeLimit(-5, 10, 1, 50)).isEqualTo(10);
        assertThat(PagingUtils.safeLimit(200, 10, 1, 50)).isEqualTo(50);
        assertThat(PagingUtils.safeLimit(5, 10, 10, 50)).isEqualTo(10);
    }

    @Test
    @DisplayName("window clamps start, limit and total consistently")
    void windowProducesConsistentBounds() {
        PagingUtils.Window window = PagingUtils.window(-5, 0, 10, 1, 40, 100);
        assertThat(window.startIndex()).isZero();
        assertThat(window.limit()).isEqualTo(10);
        assertThat(window.totalRequested()).isEqualTo(10);

        PagingUtils.Window capped = PagingUtils.window(50, 75, 10, 1, 40, 80);
        assertThat(capped.startIndex()).isEqualTo(50);
        assertThat(capped.limit()).isEqualTo(40);
        assertThat(capped.totalRequested()).isEqualTo(80);
    }
}
