package com.williamcallahan.book_recommendation_engine.util;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Optional;
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
}
