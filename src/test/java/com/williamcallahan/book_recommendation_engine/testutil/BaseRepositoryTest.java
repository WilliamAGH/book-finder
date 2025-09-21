package com.williamcallahan.book_recommendation_engine.testutil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.data.jdbc.DataJdbcTest;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Base class for repository tests with common database setup.
 * Reduces duplication across repository test classes.
 */
@DataJdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ContextConfiguration(classes = {TestDatabaseConfig.class})
public abstract class BaseRepositoryTest {

    @Autowired
    protected JdbcTemplate jdbcTemplate;

    @MockitoBean
    protected S3Client s3Client;

    /**
     * Clean up test data from a specific table.
     */
    protected void cleanTable(String tableName) {
        jdbcTemplate.execute("TRUNCATE TABLE " + tableName + " CASCADE");
    }

    /**
     * Count rows in a table.
     */
    protected int countRows(String tableName) {
        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + tableName, Integer.class);
        return count != null ? count : 0;
    }

    /**
     * Check if a record exists with a specific condition.
     */
    protected boolean recordExists(String tableName, String whereClause, Object... params) {
        String sql = "SELECT COUNT(*) FROM " + tableName + " WHERE " + whereClause;
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, params);
        return count != null && count > 0;
    }
}