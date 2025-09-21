package com.williamcallahan.book_recommendation_engine.util;

import java.util.Arrays;
import org.slf4j.Logger;

/**
 * Lightweight helpers for consistent logging of warnings and errors with optional causes.
 */
public final class LoggingUtils {
    private LoggingUtils() {
    }

    public static void error(Logger logger, Throwable throwable, String message, Object... args) {
        log(logger, LogLevel.ERROR, throwable, message, args);
    }

    public static void warn(Logger logger, Throwable throwable, String message, Object... args) {
        log(logger, LogLevel.WARN, throwable, message, args);
    }

    private static void log(Logger logger, LogLevel level, Throwable throwable, String message, Object... args) {
        if (logger == null || message == null) {
            return;
        }

        Object[] finalArgs;
        if (throwable != null) {
            Object[] base = (args == null || args.length == 0) ? new Object[0] : Arrays.copyOf(args, args.length);
            finalArgs = Arrays.copyOf(base, base.length + 1);
            finalArgs[finalArgs.length - 1] = throwable;
        } else {
            finalArgs = args;
        }

        switch (level) {
            case ERROR -> {
                if (finalArgs == null || finalArgs.length == 0) {
                    logger.error(message);
                } else {
                    logger.error(message, finalArgs);
                }
            }
            case WARN -> {
                if (finalArgs == null || finalArgs.length == 0) {
                    logger.warn(message);
                } else {
                    logger.warn(message, finalArgs);
                }
            }
        }
    }

    private enum LogLevel {
        ERROR,
        WARN
    }
}
