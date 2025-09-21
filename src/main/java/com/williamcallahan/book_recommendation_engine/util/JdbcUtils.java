package com.williamcallahan.book_recommendation_engine.util;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Shared JDBC helper methods for retrieving optional values without repeating
 * boilerplate try/catch blocks across services.
 */
public final class JdbcUtils {

    private JdbcUtils() {
    }

    /**
     * Executes the supplied query and returns the first column as an optional string.
     *
     * @param jdbcTemplate the {@link JdbcTemplate} to use; treated as absent when {@code null}
     * @param sql          SQL statement to execute
     * @param args         positional arguments for the SQL statement
     * @return optional string result
     */
    public static Optional<String> optionalString(JdbcTemplate jdbcTemplate, String sql, Object... args) {
        return optionalString(jdbcTemplate, sql, null, args);
    }

    /**
     * Executes the supplied query and returns the first column as an optional string,
     * invoking the provided failure callback when a {@link DataAccessException} occurs.
     *
     * @param jdbcTemplate the {@link JdbcTemplate} to use; treated as absent when {@code null}
     * @param sql          SQL statement to execute
     * @param onFailure    callback invoked when a {@link DataAccessException} is thrown (optional)
     * @param args         positional arguments for the SQL statement
     * @return optional string result
     */
    public static Optional<String> optionalString(JdbcTemplate jdbcTemplate,
                                                  String sql,
                                                  Consumer<DataAccessException> onFailure,
                                                  Object... args) {
        if (jdbcTemplate == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(jdbcTemplate.queryForObject(sql, String.class, args));
        } catch (DataAccessException ex) {
            if (onFailure != null) {
                onFailure.accept(ex);
            }
            return Optional.empty();
        }
    }

    /**
     * Query for an optional single result of any type, handling EmptyResultDataAccessException gracefully.
     */
    public static <T> Optional<T> queryForOptional(JdbcTemplate jdbc, String sql, Class<T> type, Object... params) {
        try {
            return Optional.ofNullable(jdbc.queryForObject(sql, type, params));
        } catch (EmptyResultDataAccessException e) {
            return Optional.empty();
        }
    }

    /**
     * Query for a UUID, returning null if not found.
     */
    public static UUID queryForUuid(JdbcTemplate jdbc, String sql, Object... params) {
        return queryForOptional(jdbc, sql, UUID.class, params).orElse(null);
    }

    /**
     * Query for an Integer, returning null if not found.
     */
    public static Integer queryForInt(JdbcTemplate jdbc, String sql, Object... params) {
        return queryForOptional(jdbc, sql, Integer.class, params).orElse(null);
    }

    /**
     * Query for a Long, returning null if not found.
     */
    public static Long queryForLong(JdbcTemplate jdbc, String sql, Object... params) {
        return queryForOptional(jdbc, sql, Long.class, params).orElse(null);
    }

    /**
     * Check if a record exists.
     */
    public static boolean exists(JdbcTemplate jdbc, String sql, Object... params) {
        Long count = queryForOptional(jdbc, "SELECT COUNT(*) FROM (" + sql + ") AS subquery", Long.class, params).orElse(0L);
        return count > 0;
    }

    /**
     * Query for a single object with a RowMapper, returning Optional.
     */
    public static <T> Optional<T> queryForOptionalObject(JdbcTemplate jdbc, String sql, RowMapper<T> rowMapper, Object... params) {
        List<T> results = jdbc.query(sql, rowMapper, params);
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    /**
     * Execute an update and return whether any rows were affected.
     */
    public static boolean executeUpdate(JdbcTemplate jdbc, String sql, Object... params) {
        return jdbc.update(sql, params) > 0;
    }
}
