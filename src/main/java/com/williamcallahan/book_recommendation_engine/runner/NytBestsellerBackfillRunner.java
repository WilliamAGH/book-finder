package com.williamcallahan.book_recommendation_engine.runner;

import com.williamcallahan.book_recommendation_engine.scheduler.NewYorkTimesBestsellerScheduler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

@Component
@Slf4j
public class NytBestsellerBackfillRunner implements CommandLineRunner {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private final ApplicationArguments arguments;
    private final NewYorkTimesBestsellerScheduler scheduler;

    public NytBestsellerBackfillRunner(ApplicationArguments arguments,
                                       NewYorkTimesBestsellerScheduler scheduler) {
        this.arguments = arguments;
        this.scheduler = scheduler;
    }

    @Override
    public void run(String... args) throws Exception {
        if (!arguments.containsOption("nyt.backfill.start")) {
            return;
        }

        LocalDate start = parseDateOption("nyt.backfill.start", true);
        LocalDate end = parseDateOption("nyt.backfill.end", false);
        if (end == null) {
            end = LocalDate.now();
        }
        if (end.isBefore(start)) {
            log.warn("NYT backfill end date {} is before start date {}; swapping.", end, start);
            LocalDate tmp = start;
            start = end;
            end = tmp;
        }

        long delayMillis = parseLongOption("nyt.backfill.delay", 0L);
        int weeksStep = (int) parseLongOption("nyt.backfill.step-weeks", 1L);
        if (weeksStep <= 0) {
            weeksStep = 1;
        }

        log.info("Starting NYT backfill from {} to {} (step {} week(s), delay {} ms).", start, end, weeksStep, delayMillis);

        LocalDate cursor = start;
        while (!cursor.isAfter(end)) {
            log.info("=== NYT backfill for {} ===", cursor);
            scheduler.processNewYorkTimesBestsellers(cursor, true);
            if (delayMillis > 0) {
                Thread.sleep(delayMillis);
            }
            cursor = cursor.plusWeeks(weeksStep);
        }

        log.info("NYT backfill completed.");
    }

    private LocalDate parseDateOption(String option, boolean required) {
        if (!arguments.containsOption(option)) {
            if (required) {
                throw new IllegalArgumentException("Missing required option --" + option + "=YYYY-MM-DD");
            }
            return null;
        }
        String raw = arguments.getOptionValues(option).get(0);
        try {
            return LocalDate.parse(raw, DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("Invalid date for --" + option + ": " + raw, ex);
        }
    }

    private long parseLongOption(String option, long defaultValue) {
        if (!arguments.containsOption(option)) {
            return defaultValue;
        }
        String raw = arguments.getOptionValues(option).get(0);
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException ex) {
            log.warn("Invalid numeric value for --{} ({}). Using default {}.", option, raw, defaultValue);
            return defaultValue;
        }
    }
}
