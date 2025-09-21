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
}
