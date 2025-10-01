package com.williamcallahan.book_recommendation_engine.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Scheduled task to maintain work_clusters for edition grouping.
 * Runs clustering functions periodically to group books into edition families.
 * 
 * @author William Callahan
 */
@Component
@ConditionalOnBean(JdbcTemplate.class)
public class WorkClusterScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkClusterScheduler.class);
    
    private final JdbcTemplate jdbcTemplate;

    public WorkClusterScheduler(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Runs work clustering every 6 hours to group newly added books into edition families.
     * Clusters books by:
     * 1. ISBN prefix (same publisher/work)
     * 2. Google Books canonical volume ID
     */
    @Scheduled(fixedDelay = 6 * 60 * 60 * 1000, initialDelay = 5 * 60 * 1000) // Every 6 hours, start after 5 min
    public void clusterBooks() {
        try {
            LOGGER.info("Starting scheduled work clustering...");
            
            // Cluster by ISBN prefix
            Integer[] isbnResults = jdbcTemplate.queryForObject(
                "SELECT * FROM cluster_books_by_isbn()",
                (rs, rowNum) -> new Integer[]{rs.getInt("clusters_created"), rs.getInt("books_clustered")}
            );
            
            // Cluster by Google canonical ID
            Integer[] googleResults = jdbcTemplate.queryForObject(
                "SELECT * FROM cluster_books_by_google_canonical()",
                (rs, rowNum) -> new Integer[]{rs.getInt("clusters_created"), rs.getInt("books_clustered")}
            );
            
            if (isbnResults != null && googleResults != null) {
                int totalClusters = isbnResults[0] + googleResults[0];
                int totalBooks = isbnResults[1] + googleResults[1];
                LOGGER.info("Work clustering completed: {} clusters created/updated, {} books grouped into editions", 
                    totalClusters, totalBooks);
            }
            
        } catch (Exception e) {
            LOGGER.error("Failed to run work clustering: {}", e.getMessage(), e);
        }
    }

    /**
     * Get current clustering statistics on demand.
     */
    public void logClusteringStats() {
        try {
            jdbcTemplate.query(
                "SELECT * FROM get_clustering_stats()",
                rs -> {
                    LOGGER.info("Clustering stats: {} total books, {} clustered, {} unclustered, {} total clusters, avg {}/cluster",
                        rs.getLong("total_books"),
                        rs.getLong("clustered_books"),
                        rs.getLong("unclustered_books"),
                        rs.getLong("total_clusters"),
                        rs.getBigDecimal("avg_cluster_size")
                    );
                }
            );
        } catch (Exception e) {
            LOGGER.warn("Failed to get clustering stats: {}", e.getMessage());
        }
    }
}
