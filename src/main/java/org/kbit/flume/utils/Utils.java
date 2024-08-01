package org.kbit.flume.utils;

import org.slf4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Utils {
    public static void logGitCommitLog(Logger logger) {
        try {
            Process process = Runtime.getRuntime().exec("git log -1 --pretty=%B%x09%ct");
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            StringBuilder commitMessage = new StringBuilder();
            String commitTimestamp = null;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                commitMessage.append(parts[0]).append("\n");
                if (parts.length > 1) {
                    commitTimestamp = parts[1];
                }
            }
            if (commitTimestamp != null) {
                long commitTime = Long.parseLong(commitTimestamp);
                LocalDateTime commitDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(commitTime), ZoneOffset.UTC);
                LocalDateTime currentDateTime = LocalDateTime.now(ZoneOffset.UTC);

                Duration duration = Duration.between(commitDateTime, currentDateTime);
                String timeAgo = formatDuration(duration);
                String formattedTimestamp = commitDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                logger.info("Last Git Commit Message:\n{}\nCommit was made: {} / {}", commitMessage.toString(), timeAgo, formattedTimestamp);
            } else {
                logger.info("Last Git Commit Message:\n{}", commitMessage.toString());
            }
        } catch (IOException e) {
            logger.error("Failed to retrieve Git commit log", e);
        }
    }

    private static String formatDuration(Duration duration) {
        long seconds = duration.getSeconds();
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;

        if (days > 0) {
            return days + " day" + (days > 1 ? "s" : "") + " ago";
        } else if (hours > 0) {
            return hours + " hour" + (hours > 1 ? "s" : "") + " ago";
        } else if (minutes > 0) {
            return minutes + " minute" + (minutes > 1 ? "s" : "") + " ago";
        } else {
            return seconds + " second" + (seconds > 1 ? "s" : "") + " ago";
        }
    }

    /**
     * Extracts the value of a specified field from a given line.
     * The field is expected to be in the format: fieldName="value".
     *
     * @param line The line from which to extract the field value.
     * @param fieldName The name of the field to extract.
     * @return The value of the specified field, or "unknown" if the field is not found.
     */
    public static String extractFieldValue(String line, String fieldName) {
        String searchPattern = fieldName + "=\"";
        int startIndex = line.indexOf(searchPattern);
        if (startIndex != -1) {
            int endIndex = line.indexOf("\"", startIndex + searchPattern.length());
            if (endIndex != -1) {
                return line.substring(startIndex + searchPattern.length(), endIndex);
            }
        }
        return "unknown";
    }

}
