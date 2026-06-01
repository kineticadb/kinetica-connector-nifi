package com.kinetica.nifi.processors;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.nifi.logging.ComponentLog;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Type.Column;

/**
 * Utility methods for Kinetica NiFi processors.
 *
 * <p>This class provides common utility functions used across all Kinetica processors:
 * <ul>
 *   <li>Date parsing with timezone support</li>
 *   <li>Column type detection</li>
 *   <li>Table existence checking</li>
 *   <li>Exception handling utilities</li>
 * </ul>
 *
 * <p>All methods are designed to be null-safe and handle error conditions gracefully.
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
public final class KineticaUtilities {

    // Prevent instantiation
    private KineticaUtilities() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }

    /**
     * Checks if a column has the TIMESTAMP property.
     *
     * @param column The column to check (can be null)
     * @return true if the column has the TIMESTAMP property, false otherwise
     */
    public static boolean checkForTimeStamp(Column column) {
        if (column == null) {
            return false;
        }

        try {
            return column.hasProperty(ColumnProperty.TIMESTAMP);
        } catch (Exception e) {
            // In case of any issues with property checking
            return false;
        }
    }

    /**
     * Converts an exception's stack trace to a string.
     *
     * @param e The exception to convert (can be null)
     * @return The stack trace as a string, or empty string if exception is null
     */
    public static String convertStacktraceToString(Exception e) {
        if (e == null) {
            return "";
        }

        StringWriter sw = new StringWriter();
        e.printStackTrace(new PrintWriter(sw));
        return sw.toString();
    }

    /**
     * Checks if a table exists in Kinetica.
     *
     * @param gpudb The GPUdb connection (can be null)
     * @param tableName The table name to check (can be null)
     * @param logger The logger for error messages (can be null)
     * @return true if the table exists, false otherwise or on error
     */
    public static boolean tableExists(GPUdb gpudb, String tableName, ComponentLog logger) {
        if (gpudb == null || tableName == null || tableName.isEmpty()) {
            return false;
        }

        try {
            return gpudb.hasTable(tableName, null).getTableExists();
        } catch (GPUdbException e) {
            if (logger != null) {
                logger.error("Failed checking if table '{}' exists: {}", tableName, e.getMessage());
            }
            return false;
        }
    }

    /**
     * Parses a date string into a Unix timestamp (milliseconds since epoch).
     *
     * <p>This method is null-safe and will return null if parsing fails or if
     * required parameters are missing.
     *
     * @param dateString The date string to parse (can be null)
     * @param dateFormat The date format pattern (e.g., "yyyy/MM/dd HH:mm:ss") - REQUIRED
     * @param timeZone The timezone ID (e.g., "EST", "UTC") - optional, defaults to system timezone
     * @param logger The logger for error messages (can be null)
     * @return The timestamp in milliseconds, or null if parsing fails
     */
    public static Long parseDate(String dateString, String dateFormat, String timeZone, ComponentLog logger) {
        // Null safety checks - FIX for null pointer exception
        if (dateString == null || dateString.isEmpty()) {
            if (logger != null) {
                logger.warn("Date string is null or empty, cannot parse");
            }
            return null;
        }

        if (dateFormat == null || dateFormat.isEmpty()) {
            if (logger != null) {
                logger.error("Date format pattern is required but was null or empty");
            }
            return null;
        }

        try {
            // Determine timezone (default to system timezone if not specified)
            TimeZone tz = (timeZone != null && !timeZone.isEmpty())
                    ? TimeZone.getTimeZone(timeZone)
                    : TimeZone.getDefault();

            // Create and configure the date parser
            SimpleDateFormat parser = new SimpleDateFormat(dateFormat);
            parser.setLenient(true);
            parser.setTimeZone(tz);

            // Parse the date
            Date date = parser.parse(dateString);
            if (date != null) {
                return date.getTime();
            }

            if (logger != null) {
                logger.warn("Failed to parse date '{}' with format '{}' - result was null", dateString, dateFormat);
            }
            return null;

        } catch (Exception e) {
            if (logger != null) {
                logger.error("Failed to parse date '{}' with format '{}' and timezone '{}': {}",
                        dateString, dateFormat, timeZone, e.getMessage());
            }
            return null;
        }
    }

    /**
     * Parses a date string into a Unix timestamp, with optional numeric fallback.
     *
     * <p>If the date string is already a numeric value, it will be parsed as a
     * timestamp directly. Otherwise, the date format will be used for parsing.
     *
     * @param dateString The date string to parse
     * @param dateFormat The date format pattern for non-numeric strings
     * @param timeZone The timezone ID (optional)
     * @param logger The logger for messages
     * @return The timestamp in milliseconds, or null if parsing fails
     */
    public static Long parseDateOrTimestamp(String dateString, String dateFormat, String timeZone, ComponentLog logger) {
        if (dateString == null || dateString.isEmpty()) {
            return null;
        }

        // Check if it's already a numeric timestamp
        if (isNumeric(dateString)) {
            try {
                return Long.parseLong(dateString);
            } catch (NumberFormatException e) {
                // Fall through to date parsing
            }
        }

        // Parse as formatted date
        return parseDate(dateString, dateFormat, timeZone, logger);
    }

    /**
     * Checks if a string contains only numeric characters.
     *
     * @param str The string to check
     * @return true if the string is numeric, false otherwise
     */
    public static boolean isNumeric(String str) {
        if (str == null || str.isEmpty()) {
            return false;
        }

        for (int i = 0; i < str.length(); i++) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Safely trims a string, returning null if the result would be empty.
     *
     * @param str The string to trim (can be null)
     * @return The trimmed string, or null if empty
     */
    public static String trimToNull(String str) {
        if (str == null) {
            return null;
        }
        String trimmed = str.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * Safely parses an integer with a default fallback.
     *
     * @param str The string to parse
     * @param defaultValue The value to return if parsing fails
     * @return The parsed integer or the default value
     */
    public static int parseIntSafe(String str, int defaultValue) {
        if (str == null || str.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(str.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Safely parses a long with a default fallback.
     *
     * @param str The string to parse
     * @param defaultValue The value to return if parsing fails
     * @return The parsed long or the default value
     */
    public static long parseLongSafe(String str, long defaultValue) {
        if (str == null || str.isEmpty()) {
            return defaultValue;
        }
        try {
            return Long.parseLong(str.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Safely parses a double with a default fallback.
     *
     * @param str The string to parse
     * @param defaultValue The value to return if parsing fails
     * @return The parsed double or the default value
     */
    public static double parseDoubleSafe(String str, double defaultValue) {
        if (str == null || str.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(str.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * Safely parses a float with a default fallback.
     *
     * @param str The string to parse
     * @param defaultValue The value to return if parsing fails
     * @return The parsed float or the default value
     */
    public static float parseFloatSafe(String str, float defaultValue) {
        if (str == null || str.isEmpty()) {
            return defaultValue;
        }
        try {
            return Float.parseFloat(str.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // ========== CHARACTER AND STRING UTILITIES ==========

    /**
     * Parses special character sequences into their actual characters.
     *
     * <p>Supported escape sequences:
     * <ul>
     *   <li>\t - tab character</li>
     *   <li>\n - newline character</li>
     *   <li>\r - carriage return</li>
     *   <li>Any single character - used as-is</li>
     * </ul>
     *
     * @param str The string to parse
     * @param defaultChar The default character if str is null or empty
     * @return The parsed character
     */
    public static char parseSpecialChar(String str, char defaultChar) {
        if (str == null || str.isEmpty()) {
            return defaultChar;
        }
        switch (str) {
            case "\\t":
                return '\t';
            case "\\n":
                return '\n';
            case "\\r":
                return '\r';
            case "\\\\":
                return '\\';
            default:
                return str.charAt(0);
        }
    }

    /**
     * Escapes a string for use in JSON output.
     *
     * <p>This method handles all JSON special characters according to RFC 8259.
     *
     * @param value The string to escape (can be null)
     * @return The escaped string, or empty string if null
     */
    public static String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\b':
                    sb.append("\\b");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < ' ') {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    /**
     * Escapes a string for use in CSV output.
     *
     * <p>If the value contains the delimiter, quote character, or newlines,
     * it will be quoted and internal quotes will be doubled.
     *
     * @param value The string to escape (can be null)
     * @param delimiter The CSV delimiter character
     * @param quoteChar The quote character (can be null to disable quoting)
     * @return The escaped string suitable for CSV output
     */
    public static String escapeCsv(String value, char delimiter, Character quoteChar) {
        if (value == null) {
            return "";
        }

        if (quoteChar == null) {
            return value;
        }

        boolean needsQuoting = value.indexOf(delimiter) >= 0 ||
                value.indexOf('\n') >= 0 ||
                value.indexOf('\r') >= 0 ||
                value.indexOf(quoteChar) >= 0;

        if (!needsQuoting) {
            return value;
        }

        StringBuilder sb = new StringBuilder(value.length() + 16);
        sb.append(quoteChar);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == quoteChar) {
                sb.append(quoteChar); // Double the quote character
            }
            sb.append(c);
        }
        sb.append(quoteChar);
        return sb.toString();
    }

    // ========== TYPE MAPPING UTILITIES ==========

    /**
     * Maps a Kinetica column type to a schema type name.
     *
     * @param columnType The Java class of the column type
     * @return The schema type name (e.g., "double", "float", "int", "long", "string")
     */
    public static String mapTypeToSchemaName(Class<?> columnType) {
        if (columnType == Double.class || columnType == Double.TYPE) {
            return "double";
        } else if (columnType == Float.class || columnType == Float.TYPE) {
            return "float";
        } else if (columnType == Integer.class || columnType == Integer.TYPE) {
            return "int";
        } else if (columnType == Long.class || columnType == Long.TYPE) {
            return "long";
        } else {
            return "string";
        }
    }
}
