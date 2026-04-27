package com.gisfederal.gpudb.processors.GPUdbNiFi;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for GetKineticaToJSON processor property validation.
 * These tests do NOT require a live Kinetica instance.
 */
public class TestGetKineticaProperties {

    private TestRunner jsonRunner;
    private TestRunner csvRunner;

    @Before
    public void setup() {
        jsonRunner = TestRunners.newTestRunner(GetKineticaToJSON.class);
        csvRunner = TestRunners.newTestRunner(GetKineticaToCSV.class);
    }

    @Test
    public void testJsonValidProperties() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        jsonRunner.assertValid();
    }

    @Test
    public void testJsonMissingServerUrlFails() {
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonMissingTableNameFails() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonMissingTableMonitorUrlFails() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonWithCredentialsValid() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        jsonRunner.setProperty(KineticaConstants.USERNAME, "admin");
        jsonRunner.setProperty(KineticaConstants.PASSWORD, "secret");
        jsonRunner.assertValid();
    }

    @Test
    public void testCsvValidProperties() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        csvRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        csvRunner.assertValid();
    }

    @Test
    public void testCsvMissingServerUrlFails() {
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        csvRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        csvRunner.assertNotValid();
    }

    @Test
    public void testCsvWithCustomDelimiterValid() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        csvRunner.setProperty(KineticaConstants.TABLE_MONITOR_URL, "tcp://localhost:9002");
        csvRunner.setProperty(KineticaConstants.DELIMITER, "|");
        csvRunner.assertValid();
    }
}
