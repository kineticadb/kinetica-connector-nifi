package com.gisfederal.gpudb.processors.GPUdbNiFi;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for PutKineticaFromFile processor property validation.
 * These tests do NOT require a live Kinetica instance.
 */
public class TestPutKineticaFromFileProperties {

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(PutKineticaFromFile.class);
    }

    @Test
    public void testValidPropertiesPass() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertValid();
    }

    @Test
    public void testMissingServerUrlFails() {
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testMissingTableNameFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testTabDelimiterValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        runner.setProperty(KineticaConstants.DELIMITER, "\t");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertValid();
    }

    @Test
    public void testPipeDelimiterValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, "|");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertValid();
    }

    @Test
    public void testOptionalQuoteCharacterValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.QUOTE_CHARACTER, "'");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertValid();
    }

    @Test
    public void testEmptyQuoteCharacterValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.QUOTE_CHARACTER, "");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertValid();
    }

    @Test
    public void testDateFormatAndTimezoneValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.setProperty(KineticaConstants.DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
        runner.setProperty(KineticaConstants.TIMEZONE, "UTC");
        runner.assertValid();
    }

    @Test
    public void testWithCredentialsValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.setProperty(KineticaConstants.USERNAME, "admin");
        runner.setProperty(KineticaConstants.PASSWORD, "password123");
        runner.assertValid();
    }

    @Test
    public void testHeaderFlagValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.setProperty(KineticaConstants.FILE_HAS_HEADER, "false");
        runner.assertValid();
    }
}
