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
     * Converts a string to a UUID, returning null if the string is null or not a valid UUID.
     */
    public static UUID toUuid(String uuidString) {
        if (uuidString == null) {
            return null;
        }
        try {
            return UUID.fromString(uuidString);
        } catch (IllegalArgumentException e) {
            return null;
        }
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
        try {
            return jdbc.update(sql, params) > 0;
        } catch (DataAccessException ex) {
            // Log detailed error for debugging SQL issues
            System.err.println("SQL Error: " + ex.getMessage());
            System.err.println("SQL: " + sql);
            System.err.println("Params: " + java.util.Arrays.toString(params));
            if (ex.getCause() != null) {
                System.err.println("Root cause: " + ex.getCause().getMessage());
            }
            throw ex;
        }
    }

    /**
     * Builds a COALESCE expression for upsert operations.
     * Useful for ON CONFLICT DO UPDATE SET clauses where you want to preserve existing non-null values.
     *
     * @param column the column name
     * @param useExcluded true to use EXCLUDED.column as the new value, false to keep existing
     * @return the COALESCE expression
     */
    public static String buildCoalesceExpression(String column, boolean useExcluded) {
        if (useExcluded) {
            return String.format("COALESCE(EXCLUDED.%s, %s.%s)", column, getTableAlias(column), column);
        } else {
            return String.format("%s.%s", getTableAlias(column), column);
        }
    }

    /**
     * Builds a COALESCE upsert SET clause for multiple columns.
     * Each column will use COALESCE(EXCLUDED.column, table.column) pattern.
     *
     * @param tableName the table name for the existing values reference
     * @param columns the columns to include in the SET clause
     * @return the complete SET clause
     */
    public static String buildCoalesceSetClause(String tableName, String... columns) {
        if (columns == null || columns.length == 0) {
            return "";
        }

        StringBuilder setClause = new StringBuilder();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                setClause.append(", ");
            }
            String column = columns[i];
            setClause.append(column)
                    .append(" = COALESCE(EXCLUDED.")
                    .append(column)
                    .append(", ")
                    .append(tableName)
                    .append(".")
                    .append(column)
                    .append(")");
        }
        return setClause.toString();
    }

    /**
     * Builds an upsert query with COALESCE pattern for preserving existing values.
     * This is a template method for common upsert patterns.
     *
     * @param tableName the target table
     * @param insertColumns columns for the INSERT part
     * @param conflictColumns columns for the ON CONFLICT part
     * @param updateColumns columns to update with COALESCE pattern
     * @return the complete upsert SQL statement
     */
    public static String buildUpsertQuery(String tableName,
                                          String[] insertColumns,
                                          String[] conflictColumns,
                                          String[] updateColumns) {
        StringBuilder sql = new StringBuilder();

        // INSERT INTO table (columns...)
        sql.append("INSERT INTO ").append(tableName).append(" (");
        sql.append(String.join(", ", insertColumns));
        sql.append(") VALUES (");

        // Add parameter placeholders
        for (int i = 0; i < insertColumns.length; i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(") ");

        // ON CONFLICT (columns...)
        sql.append("ON CONFLICT (");
        sql.append(String.join(", ", conflictColumns));
        sql.append(") DO UPDATE SET ");

        // Build COALESCE updates
        sql.append(buildCoalesceSetClause(tableName, updateColumns));

        // Add updated_at if not already included
        boolean hasUpdatedAt = false;
        for (String col : updateColumns) {
            if ("updated_at".equalsIgnoreCase(col)) {
                hasUpdatedAt = true;
                break;
            }
        }
        if (!hasUpdatedAt) {
            sql.append(", updated_at = NOW()");
        }

        return sql.toString();
    }

    /**
     * Helper to get table alias from column name (used internally).
     */
    private static String getTableAlias(String column) {
        // For most cases, we'll use the table name from context
        // This is a simplified version - in practice would be passed in
        return column.contains(".") ? column.substring(0, column.indexOf(".")) : "t";
    }

    /**
     * Executes an upsert with automatic COALESCE pattern.
     *
     * @param jdbc the JdbcTemplate
     * @param tableName the target table
     * @param insertColumns columns for INSERT
     * @param conflictColumns columns for ON CONFLICT
     * @param updateColumns columns to update with COALESCE
     * @param values the values to insert/update
     * @return true if rows were affected
     */
    public static boolean executeUpsert(JdbcTemplate jdbc,
                                        String tableName,
                                        String[] insertColumns,
                                        String[] conflictColumns,
                                        String[] updateColumns,
                                        Object... values) {
        String sql = buildUpsertQuery(tableName, insertColumns, conflictColumns, updateColumns);
        return executeUpdate(jdbc, sql, values);
    }
}
