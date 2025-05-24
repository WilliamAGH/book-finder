package com.williamcallahan.book_recommendation_engine.service;

import com.williamcallahan.book_recommendation_engine.types.DryRunSummary;
import com.williamcallahan.book_recommendation_engine.types.MoveActionSummary;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.williamcallahan.book_recommendation_engine.service.image.ImageProcessingService;

class S3CoverCleanupServiceTest {

    private S3CoverCleanupService cleanupService;
    private S3StorageService s3StorageService;
    private ImageProcessingService imageProcessingService;

    @BeforeEach
    void setUp() {
        s3StorageService = mock(S3StorageService.class);
        imageProcessingService = mock(ImageProcessingService.class);
        cleanupService = new S3CoverCleanupService(s3StorageService, imageProcessingService);
    }

    @Test
    void testPerformDryRun_noBucketConfigured() throws Exception {
        when(s3StorageService.getBucketName()).thenReturn(null);
        DryRunSummary summary = cleanupService.performDryRun("prefix/", 10).get();
        assertEquals(0, summary.getTotalScanned());
        assertEquals(0, summary.getTotalFlagged());
        assertTrue(summary.getFlaggedFileKeys().isEmpty());
    }

    @Test
    void testPerformDryRun_flagsDominantlyWhite() throws Exception {
        when(s3StorageService.getBucketName()).thenReturn("bucket");
        S3Object obj1 = S3Object.builder().key("key1.jpg").size(100L).build();
        S3Object obj2 = S3Object.builder().key("key2.jpg").size(50L).build();
        when(s3StorageService.listObjectsAsync("prefix/")).thenReturn(
            CompletableFuture.completedFuture(Arrays.asList(obj1, obj2))
        );
        when(s3StorageService.downloadFileAsBytesAsync(eq("key1.jpg")))
            .thenReturn(CompletableFuture.completedFuture(new byte[]{1}));
        when(s3StorageService.downloadFileAsBytesAsync(eq("key2.jpg")))
            .thenReturn(CompletableFuture.completedFuture(new byte[]{1}));
        when(imageProcessingService.isDominantlyWhiteAsync(any(byte[].class), eq("key1.jpg")))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(imageProcessingService.isDominantlyWhiteAsync(any(byte[].class), eq("key2.jpg")))
            .thenReturn(CompletableFuture.completedFuture(false));

        DryRunSummary summary = cleanupService.performDryRun("prefix/", 0).get();
        assertEquals(2, summary.getTotalScanned());
        assertEquals(1, summary.getTotalFlagged());
        assertEquals(Collections.singletonList("key1.jpg"), summary.getFlaggedFileKeys());
    }

    @Test
    void testPerformMoveAction_successfulMove() throws Exception {
        when(s3StorageService.getBucketName()).thenReturn("bucket");
        S3Object obj = S3Object.builder().key("prefix/file1.jpg").size(200L).build();
        when(s3StorageService.listObjectsAsync("prefix/"))
            .thenReturn(CompletableFuture.completedFuture(Collections.singletonList(obj)));
        when(s3StorageService.downloadFileAsBytesAsync(eq("prefix/file1.jpg")))
            .thenReturn(CompletableFuture.completedFuture(new byte[]{2}));
        when(imageProcessingService.isDominantlyWhiteAsync(any(byte[].class), eq("prefix/file1.jpg")))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(s3StorageService.copyObjectAsync(eq("prefix/file1.jpg"), eq("quarantine/file1.jpg")))
            .thenReturn(CompletableFuture.completedFuture(true));
        when(s3StorageService.deleteObjectAsync(eq("prefix/file1.jpg")))
            .thenReturn(CompletableFuture.completedFuture(true));

        MoveActionSummary summary = cleanupService.performMoveAction("prefix/", 0, "quarantine").get();
        assertEquals(1, summary.getTotalScanned());
        assertEquals(1, summary.getTotalFlagged());
        assertEquals(1, summary.getSuccessfullyMoved());
        assertEquals(0, summary.getFailedToMove());
        assertEquals(Collections.singletonList("prefix/file1.jpg"), summary.getFlaggedFileKeys());
        assertEquals(Collections.singletonList("prefix/file1.jpg -> quarantine/file1.jpg"), summary.getMovedFileKeys());
        assertTrue(summary.getFailedMoveFileKeys().isEmpty());
    }
} 