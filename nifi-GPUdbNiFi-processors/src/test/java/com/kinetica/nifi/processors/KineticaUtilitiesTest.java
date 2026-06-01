package com.kinetica.nifi.processors;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for KineticaUtilities helper methods.
 * These tests verify null-safety and correct type parsing.
 */
public class KineticaUtilitiesTest {

    // ========== trimToNull tests ==========

    @Test
    public void testTrimToNull_withNull() {
        assertNull(KineticaUtilities.trimToNull(null));
    }

    @Test
    public void testTrimToNull_withEmptyString() {
        assertNull(KineticaUtilities.trimToNull(""));
    }

    @Test
    public void testTrimToNull_withWhitespaceOnly() {
        assertNull(KineticaUtilities.trimToNull("   "));
        assertNull(KineticaUtilities.trimToNull("\t\n"));
    }

    @Test
    public void testTrimToNull_withValidString() {
        assertEquals("hello", KineticaUtilities.trimToNull("hello"));
        assertEquals("hello", KineticaUtilities.trimToNull("  hello  "));
    }

    // ========== parseIntSafe tests ==========

    @Test
    public void testParseIntSafe_withValidInt() {
        assertEquals(123, KineticaUtilities.parseIntSafe("123", 0));
        assertEquals(-456, KineticaUtilities.parseIntSafe("-456", 0));
    }

    @Test
    public void testParseIntSafe_withNull() {
        assertEquals(99, KineticaUtilities.parseIntSafe(null, 99));
    }

    @Test
    public void testParseIntSafe_withEmpty() {
        assertEquals(99, KineticaUtilities.parseIntSafe("", 99));
    }

    @Test
    public void testParseIntSafe_withInvalidString() {
        assertEquals(0, KineticaUtilities.parseIntSafe("abc", 0));
        assertEquals(-1, KineticaUtilities.parseIntSafe("12.34", -1));
    }

    // ========== parseLongSafe tests ==========

    @Test
    public void testParseLongSafe_withValidLong() {
        assertEquals(123456789012L, KineticaUtilities.parseLongSafe("123456789012", 0L));
        assertEquals(-999L, KineticaUtilities.parseLongSafe("-999", 0L));
    }

    @Test
    public void testParseLongSafe_withNull() {
        assertEquals(100L, KineticaUtilities.parseLongSafe(null, 100L));
    }

    @Test
    public void testParseLongSafe_withInvalidString() {
        assertEquals(0L, KineticaUtilities.parseLongSafe("not-a-number", 0L));
    }

    // ========== parseDoubleSafe tests ==========

    @Test
    public void testParseDoubleSafe_withValidDouble() {
        assertEquals(123.456, KineticaUtilities.parseDoubleSafe("123.456", 0.0), 0.0001);
        assertEquals(-99.9, KineticaUtilities.parseDoubleSafe("-99.9", 0.0), 0.0001);
    }

    @Test
    public void testParseDoubleSafe_withNull() {
        assertEquals(1.5, KineticaUtilities.parseDoubleSafe(null, 1.5), 0.0001);
    }

    @Test
    public void testParseDoubleSafe_withInvalidString() {
        assertEquals(0.0, KineticaUtilities.parseDoubleSafe("xyz", 0.0), 0.0001);
    }

    // ========== parseFloatSafe tests ==========

    @Test
    public void testParseFloatSafe_withValidFloat() {
        assertEquals(12.5f, KineticaUtilities.parseFloatSafe("12.5", 0.0f), 0.0001f);
    }

    @Test
    public void testParseFloatSafe_withNull() {
        assertEquals(3.14f, KineticaUtilities.parseFloatSafe(null, 3.14f), 0.0001f);
    }

    @Test
    public void testParseFloatSafe_withInvalidString() {
        assertEquals(0.0f, KineticaUtilities.parseFloatSafe("not-float", 0.0f), 0.0001f);
    }

    // ========== isNumeric tests ==========
    // Note: isNumeric() only checks for pure digits (0-9), not negative or decimals

    @Test
    public void testIsNumeric_withValidNumbers() {
        assertTrue(KineticaUtilities.isNumeric("123"));
        assertTrue(KineticaUtilities.isNumeric("0"));
        assertTrue(KineticaUtilities.isNumeric("999999999"));
    }

    @Test
    public void testIsNumeric_withInvalidStrings() {
        assertFalse(KineticaUtilities.isNumeric(null));
        assertFalse(KineticaUtilities.isNumeric(""));
        assertFalse(KineticaUtilities.isNumeric("abc"));
        assertFalse(KineticaUtilities.isNumeric("12abc"));
        // isNumeric only checks for digits, so these are false:
        assertFalse(KineticaUtilities.isNumeric("-456"));
        assertFalse(KineticaUtilities.isNumeric("12.34"));
        assertFalse(KineticaUtilities.isNumeric(".5"));
    }

    // ========== parseDateOrTimestamp tests ==========

    @Test
    public void testParseDateOrTimestamp_withEpochMillis() {
        // Already epoch millis should pass through
        Long result = KineticaUtilities.parseDateOrTimestamp("1609459200000", null, null, null);
        assertEquals(Long.valueOf(1609459200000L), result);
    }

    @Test
    public void testParseDateOrTimestamp_withNull() {
        assertNull(KineticaUtilities.parseDateOrTimestamp(null, null, null, null));
    }

    @Test
    public void testParseDateOrTimestamp_withEmpty() {
        assertNull(KineticaUtilities.parseDateOrTimestamp("", null, null, null));
    }

    @Test
    public void testParseDateOrTimestamp_withCustomFormat() {
        // Custom format
        Long result = KineticaUtilities.parseDateOrTimestamp("01/01/2021", "MM/dd/yyyy", null, null);
        assertNotNull(result);
    }

    @Test
    public void testParseDateOrTimestamp_withDateTimeFormat() {
        Long result = KineticaUtilities.parseDateOrTimestamp("2021-01-01 12:00:00", "yyyy-MM-dd HH:mm:ss", "UTC", null);
        assertNotNull(result);
    }

    @Test
    public void testParseDateOrTimestamp_withNoFormatFails() {
        // Without a date format, non-numeric strings return null
        Long result = KineticaUtilities.parseDateOrTimestamp("2021-01-01T00:00:00", null, null, null);
        assertNull(result);
    }
}
