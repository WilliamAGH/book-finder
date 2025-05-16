package com.williamcallahan.book_recommendation_engine.scheduler;

import com.williamcallahan.book_recommendation_engine.service.BookSitemapService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class SitemapUpdateSchedulerTest {

    @Mock
    private BookSitemapService bookSitemapService;

    @Mock
    private Logger logger; // Mocking the logger

    @InjectMocks
    private SitemapUpdateScheduler sitemapUpdateScheduler;

    @BeforeEach
    void setUp() {
        // Inject the mocked logger into the scheduler instance
        ReflectionTestUtils.setField(sitemapUpdateScheduler, "logger", logger);
    }

    @Test
    void scheduleSitemapBookIdUpdate_shouldCallServiceAndUpdateLogs_whenSuccessful() {
        // Arrange
        doNothing().when(bookSitemapService).updateAccumulatedBookIdsInS3();

        // Act
        sitemapUpdateScheduler.scheduleSitemapBookIdUpdate();

        // Assert
        verify(bookSitemapService, times(1)).updateAccumulatedBookIdsInS3();
        verify(logger, times(1)).info("Scheduler triggered: Updating accumulated book IDs in S3.");
        verify(logger, times(1)).info("Scheduler finished: Accumulated book ID update process completed.");
        verify(logger, never()).error(anyString(), any(Exception.class));
    }

    @Test
    void scheduleSitemapBookIdUpdate_shouldCatchAndLogException_whenServiceThrowsException() {
        // Arrange
        RuntimeException testException = new RuntimeException("Test S3 service error");
        doThrow(testException).when(bookSitemapService).updateAccumulatedBookIdsInS3();

        // Act
        sitemapUpdateScheduler.scheduleSitemapBookIdUpdate();

        // Assert
        verify(bookSitemapService, times(1)).updateAccumulatedBookIdsInS3();
        verify(logger, times(1)).info("Scheduler triggered: Updating accumulated book IDs in S3.");
        verify(logger, times(1)).error("Error during scheduled sitemap book ID update:", testException);
        verify(logger, never()).info("Scheduler finished: Accumulated book ID update process completed.");
    }

    @Test
    void scheduleSitemapBookIdUpdate_shouldNotPropagateException_whenServiceThrowsException() {
        // Arrange
        RuntimeException testException = new RuntimeException("Test S3 service error");
        doThrow(testException).when(bookSitemapService).updateAccumulatedBookIdsInS3();

        // Act & Assert
        assertDoesNotThrow(() -> {
            sitemapUpdateScheduler.scheduleSitemapBookIdUpdate();
        }, "The scheduler should catch the exception and not let it propagate.");
        
        // Also verify logging as a sanity check for this test's purpose
        verify(logger, times(1)).error(eq("Error during scheduled sitemap book ID update:"), eq(testException));
    }
}
