package com.kinetica.nifi.processors;

import com.kinetica.nifi.processors.base.AbstractGetKineticaProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for GetKineticaToCSV processor.
 * Tests property validation and output format configuration.
 */
public class GetKineticaToCSVTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(GetKineticaToCSV.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.assertNotValid(); // Still missing table monitor URL

        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");
        testRunner.assertValid();
    }

    @Test
    public void testDelimiterProperty() {
        testRunner.setProperty(GetKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToCSV.PROP_TABLE, "test_table");
        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");

        // Tab (default)
        testRunner.setProperty(GetKineticaToCSV.PROP_DELIMITER, "\t");
        testRunner.assertValid();

        // Comma
        testRunner.setProperty(GetKineticaToCSV.PROP_DELIMITER, ",");
        testRunner.assertValid();

        // Pipe
        testRunner.setProperty(GetKineticaToCSV.PROP_DELIMITER, "|");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        GetKineticaToCSV processor = (GetKineticaToCSV) testRunner.getProcessor();

        // Get processors only have success relationship
        assertTrue(processor.getRelationships().contains(AbstractGetKineticaProcessor.REL_SUCCESS));
        assertEquals(1, processor.getRelationships().size());
    }

    @Test
    public void testTypicalConfiguration() {
        testRunner.setProperty(GetKineticaToCSV.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToCSV.PROP_TABLE, "sensor_data");
        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");
        testRunner.setProperty(GetKineticaToCSV.PROP_DELIMITER, ",");

        testRunner.assertValid();
    }

    @Test
    public void testWithCredentials() {
        testRunner.setProperty(GetKineticaToCSV.PROP_SERVER, "https://kinetica.example.com:8082");
        testRunner.setProperty(GetKineticaToCSV.PROP_TABLE, "secure_data");
        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://kinetica.example.com:9002");
        testRunner.setProperty(GetKineticaToCSV.PROP_USERNAME, "admin");
        testRunner.setProperty(GetKineticaToCSV.PROP_PASSWORD, "secret");

        testRunner.assertValid();
    }
}
