package com.williamcallahan.book_recommendation_engine.service.s3;

import java.util.List;
import java.util.ArrayList;

public class DryRunSummary {
    private final int totalScanned;
    private final int totalFlagged;
    private final List<String> flaggedFileKeys;

    public DryRunSummary(int totalScanned, int totalFlagged, List<String> flaggedFileKeys) {
        this.totalScanned = totalScanned;
        this.totalFlagged = totalFlagged;
        this.flaggedFileKeys = flaggedFileKeys != null ? new ArrayList<>(flaggedFileKeys) : new ArrayList<>();
    }

    public int getTotalScanned() {
        return totalScanned;
    }

    public int getTotalFlagged() {
        return totalFlagged;
    }

    public List<String> getFlaggedFileKeys() {
        return new ArrayList<>(flaggedFileKeys); // Return a copy for immutability
    }

    @Override
    public String toString() {
        return "DryRunSummary{" +
               "totalScanned=" + totalScanned +
               ", totalFlagged=" + totalFlagged +
               ", flaggedFileKeysCount=" + (flaggedFileKeys != null ? flaggedFileKeys.size() : 0) +
               '}';
    }
}
