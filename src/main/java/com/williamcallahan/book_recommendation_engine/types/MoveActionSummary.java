/**
 * Summary data transfer object for file move operations on S3 storage
 * Tracks statistics and outcomes of bulk file movement operations
 * Provides detailed reporting for cleanup and maintenance tasks
 *
 * @author William Callahan
 *
 * Features:
 * - Tracks file scanning and flagging statistics for move operations
 * - Reports successful and failed file moves with detailed key lists
 * - Provides error handling and reporting for bulk operations
 * - Immutable data structure with defensive copying for thread safety
 */

package com.williamcallahan.book_recommendation_engine.types;

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
    private String errorMessage;

    public MoveActionSummary(int totalScanned, int totalFlagged, int successfullyMoved, int failedToMove,
                             List<String> flaggedFileKeys, List<String> movedFileKeys, List<String> failedMoveFileKeys) {
        this.totalScanned = totalScanned;
        this.totalFlagged = totalFlagged;
        this.successfullyMoved = successfullyMoved;
        this.failedToMove = failedToMove;
        this.flaggedFileKeys = flaggedFileKeys != null ? new ArrayList<>(flaggedFileKeys) : new ArrayList<>();
        this.movedFileKeys = movedFileKeys != null ? new ArrayList<>(movedFileKeys) : new ArrayList<>();
        this.failedMoveFileKeys = failedMoveFileKeys != null ? new ArrayList<>(failedMoveFileKeys) : new ArrayList<>();
        this.errorMessage = null;
    }

    public MoveActionSummary(String errorMessage) {
        this.totalScanned = 0;
        this.totalFlagged = 0;
        this.successfullyMoved = 0;
        this.failedToMove = 0;
        this.flaggedFileKeys = new ArrayList<>();
        this.movedFileKeys = new ArrayList<>();
        this.failedMoveFileKeys = new ArrayList<>();
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
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
               (errorMessage != null ? ", errorMessage='" + errorMessage + '\'' : "") +
               '}';
    }
}
