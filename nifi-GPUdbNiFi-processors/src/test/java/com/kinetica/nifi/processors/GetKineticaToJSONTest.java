package com.kinetica.nifi.processors;

import com.kinetica.nifi.processors.base.AbstractGetKineticaProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for GetKineticaToJSON processor.
 * Tests property validation and JSON output configuration.
 */
public class GetKineticaToJSONTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(GetKineticaToJSON.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToJSON.PROP_TABLE, "test_table");
        testRunner.assertNotValid(); // Still missing table monitor URL

        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        GetKineticaToJSON processor = (GetKineticaToJSON) testRunner.getProcessor();

        // Get processors only have success relationship
        assertTrue(processor.getRelationships().contains(AbstractGetKineticaProcessor.REL_SUCCESS));
        assertEquals(1, processor.getRelationships().size());
    }

    @Test
    public void testTypicalConfiguration() {
        testRunner.setProperty(GetKineticaToJSON.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToJSON.PROP_TABLE, "events");
        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");

        testRunner.assertValid();
    }

    @Test
    public void testWithCredentials() {
        testRunner.setProperty(GetKineticaToJSON.PROP_SERVER, "https://kinetica.example.com:8082");
        testRunner.setProperty(GetKineticaToJSON.PROP_TABLE, "secure_data");
        testRunner.setProperty(AbstractGetKineticaProcessor.PROP_TABLE_MONITOR_URL, "tcp://kinetica.example.com:9002");
        testRunner.setProperty(GetKineticaToJSON.PROP_USERNAME, "admin");
        testRunner.setProperty(GetKineticaToJSON.PROP_PASSWORD, "secret");

        testRunner.assertValid();
    }
}
