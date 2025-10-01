package com.williamcallahan.book_recommendation_engine.service.s3;

import java.util.List;
import java.util.ArrayList;

public class MoveActionSummary {
    private final int totalScanned;
    private final int totalFlagged;
    private final int successfullyMoved;
    private final int failedToMove;
    private final List<String> flaggedFileKeys;
    private final List<String> movedFileKeys;
    private final List<String> failedMoveFileKeys;

    public MoveActionSummary(int totalScanned, int totalFlagged, int successfullyMoved, int failedToMove,
                             List<String> flaggedFileKeys, List<String> movedFileKeys, List<String> failedMoveFileKeys) {
        this.totalScanned = totalScanned;
        this.totalFlagged = totalFlagged;
        this.successfullyMoved = successfullyMoved;
        this.failedToMove = failedToMove;
        this.flaggedFileKeys = flaggedFileKeys != null ? new ArrayList<>(flaggedFileKeys) : new ArrayList<>();
        this.movedFileKeys = movedFileKeys != null ? new ArrayList<>(movedFileKeys) : new ArrayList<>();
        this.failedMoveFileKeys = failedMoveFileKeys != null ? new ArrayList<>(failedMoveFileKeys) : new ArrayList<>();
    }

    public int getTotalScanned() {
        return totalScanned;
    }

    public int getTotalFlagged() {
        return totalFlagged;
    }

    public int getSuccessfullyMoved() {
        return successfullyMoved;
    }

    public int getFailedToMove() {
        return failedToMove;
    }

    public List<String> getFlaggedFileKeys() {
        return new ArrayList<>(flaggedFileKeys);
    }

    public List<String> getMovedFileKeys() {
        return new ArrayList<>(movedFileKeys);
    }

    public List<String> getFailedMoveFileKeys() {
        return new ArrayList<>(failedMoveFileKeys);
    }

    @Override
    public String toString() {
        return "MoveActionSummary{" +
               "totalScanned=" + totalScanned +
               ", totalFlagged=" + totalFlagged +
               ", successfullyMoved=" + successfullyMoved +
               ", failedToMove=" + failedToMove +
               ", flaggedFileKeysCount=" + flaggedFileKeys.size() +
               ", movedFileKeysCount=" + movedFileKeys.size() +
               ", failedMoveFileKeysCount=" + failedMoveFileKeys.size() +
               '}';
    }
}
