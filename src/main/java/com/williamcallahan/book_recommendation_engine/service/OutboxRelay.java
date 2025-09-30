package com.williamcallahan.book_recommendation_engine.service;

import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

/**
 * Relays events from transactional outbox table to WebSocket clients.
 * <p>
 * Implements the Transactional Outbox Pattern:
 * 1. Services write events to events_outbox table (same transaction as business logic)
 * 2. This relay polls the outbox and publishes to WebSocket
 * 3. Successfully sent events are marked with sent_at timestamp
 * <p>
 * Benefits:
 * - Guaranteed event delivery (transactional with database writes)
 * - No lost events if WebSocket publish fails
 * - Automatic retry for failed publishes
 * - Decouples event production from delivery
 * <p>
 * Processing:
 * - Runs every 1 second via @Scheduled
 * - Fetches up to 100 unsent events per batch
 * - Publishes to WebSocket via SimpMessagingTemplate
 * - Marks successful events with sent_at = NOW()
 * <p>
 * Example event flow:
 * <pre>
 * BookUpsertService → INSERT INTO events_outbox (SAME TX)
 *                  ↓
 * OutboxRelay (every 1s) → Poll unsent events
 *                        → Publish to /topic/book.{id}
 *                        → Mark as sent
 * </pre>
 * <p>
 * Topics:
 * - /topic/book.{bookId} - Book upsert events
 * - /topic/search.{searchId} - Search result updates
 * <p>
 * Monitoring:
 * Use getOutboxStats() to monitor pending/sent events.
 */
@Service
@Slf4j
public class OutboxRelay {
    
    private final JdbcTemplate jdbcTemplate;
    private final SimpMessagingTemplate messagingTemplate;
    
    // Batch size for processing
    private static final int BATCH_SIZE = 100;
    
    // Processing interval (1 second for near-real-time)
    private static final long PROCESS_INTERVAL_MS = 1000;
    
    public OutboxRelay(JdbcTemplate jdbcTemplate, SimpMessagingTemplate messagingTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.messagingTemplate = messagingTemplate;
    }
    
    /**
     * Process outbox events and relay to WebSocket.
     * <p>
     * Runs every 1 second via @Scheduled.
     * Processes up to 100 events per batch.
     * <p>
     * For each event:
     * 1. Fetch from events_outbox WHERE sent_at IS NULL
     * 2. Publish to WebSocket topic
     * 3. Mark as sent (UPDATE sent_at = NOW())
     * <p>
     * If publish fails, event remains unsent and will be retried.
     */
    @Scheduled(fixedDelay = PROCESS_INTERVAL_MS)
    @Async
    public void relayEvents() {
        try {
            List<OutboxEvent> events = fetchUnsentEvents(BATCH_SIZE);
            
            if (events.isEmpty()) {
                return;
            }
            
            log.debug("Relaying {} outbox events to WebSocket", events.size());
            
            for (OutboxEvent event : events) {
                try {
                    // Publish to WebSocket
                    messagingTemplate.convertAndSend(event.getTopic(), event.getPayload());
                    
                    // Mark as sent
                    markSent(event.getEventId());
                    
                    log.debug("Relayed event {} to topic {}", event.getEventId(), event.getTopic());
                } catch (Exception e) {
                    log.warn("Failed to relay event {} to topic {}: {}",
                        event.getEventId(),
                        event.getTopic(),
                        e.getMessage()
                    );
                    
                    // Increment retry count
                    incrementRetryCount(event.getEventId());
                }
            }
        } catch (Exception e) {
            log.error("Error in outbox relay processor", e);
        }
    }
    
    /**
     * Fetch unsent events ordered by creation time.
     */
    private List<OutboxEvent> fetchUnsentEvents(int limit) {
        try {
            return jdbcTemplate.query(
                """
                SELECT event_id, topic, payload, retry_count
                FROM events_outbox
                WHERE sent_at IS NULL
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (rs, rowNum) -> new OutboxEvent(
                    (UUID) rs.getObject("event_id"),
                    rs.getString("topic"),
                    rs.getString("payload"),
                    rs.getInt("retry_count")
                ),
                limit
            );
        } catch (Exception e) {
            log.error("Error fetching unsent events", e);
            return List.of();
        }
    }
    
    /**
     * Mark event as successfully sent.
     */
    private void markSent(UUID eventId) {
        try {
            jdbcTemplate.update(
                "UPDATE events_outbox SET sent_at = NOW() WHERE event_id = ?",
                eventId
            );
        } catch (Exception e) {
            log.error("Error marking event {} as sent", eventId, e);
        }
    }
    
    /**
     * Increment retry count for failed event.
     * Events with retry_count > 10 might need manual intervention.
     */
    private void incrementRetryCount(UUID eventId) {
        try {
            jdbcTemplate.update(
                "UPDATE events_outbox SET retry_count = retry_count + 1 WHERE event_id = ?",
                eventId
            );
        } catch (Exception e) {
            log.error("Error incrementing retry count for event {}", eventId, e);
        }
    }
    
    /**
     * Get outbox statistics.
     * Useful for monitoring and alerting.
     */
    public OutboxStats getOutboxStats() {
        try {
            return jdbcTemplate.queryForObject(
                """
                SELECT
                    COUNT(*) FILTER (WHERE sent_at IS NULL) as unsent,
                    COUNT(*) FILTER (WHERE sent_at IS NOT NULL) as sent,
                    COUNT(*) FILTER (WHERE sent_at IS NULL AND retry_count > 5) as stuck
                FROM events_outbox
                WHERE created_at > NOW() - INTERVAL '1 hour'
                """,
                (rs, rowNum) -> new OutboxStats(
                    rs.getInt("unsent"),
                    rs.getInt("sent"),
                    rs.getInt("stuck")
                )
            );
        } catch (Exception e) {
            log.error("Error fetching outbox stats", e);
            return new OutboxStats(0, 0, 0);
        }
    }
    
    /**
     * Clean up old sent events.
     * Call periodically to prevent table bloat.
     * Keeps last 7 days of events for debugging.
     */
    public int cleanupOldEvents() {
        try {
            return jdbcTemplate.update(
                "DELETE FROM events_outbox WHERE sent_at < NOW() - INTERVAL '7 days'"
            );
        } catch (Exception e) {
            log.error("Error cleaning up old events", e);
            return 0;
        }
    }
    
    /**
     * Retry stuck events (retry_count > 5).
     * Resets retry count to 0 for manual intervention.
     */
    public int retryStuckEvents() {
        try {
            return jdbcTemplate.update(
                "UPDATE events_outbox SET retry_count = 0 WHERE sent_at IS NULL AND retry_count > 5"
            );
        } catch (Exception e) {
            log.error("Error retrying stuck events", e);
            return 0;
        }
    }
    
    /**
     * Outbox event data.
     */
    @Value
    private static class OutboxEvent {
        UUID eventId;
        String topic;
        String payload;
        int retryCount;
    }
    
    /**
     * Outbox statistics for monitoring.
     */
    public record OutboxStats(
        int unsent,
        int sent,
        int stuck
    ) {}
}
