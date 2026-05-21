package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for GetKineticaToAvro processor.
 * Tests property validation and Avro output configuration options.
 */
public class GetKineticaToAvroTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(GetKineticaToAvro.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.assertNotValid();

        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");
        testRunner.assertValid();
    }

    @Test
    public void testBatchSizeProperty() {
        testRunner.setProperty(GetKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");

        // Small batch size
        testRunner.setProperty(GetKineticaToAvro.PROP_BATCH_SIZE, "10");
        testRunner.assertValid();

        // Large batch size
        testRunner.setProperty(GetKineticaToAvro.PROP_BATCH_SIZE, "10000");
        testRunner.assertValid();
    }

    @Test
    public void testIncludeSchemaProperty() {
        testRunner.setProperty(GetKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");

        // Include schema (default)
        testRunner.setProperty(GetKineticaToAvro.PROP_INCLUDE_SCHEMA, "true");
        testRunner.assertValid();

        // Exclude schema
        testRunner.setProperty(GetKineticaToAvro.PROP_INCLUDE_SCHEMA, "false");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalConfiguration() {
        testRunner.setProperty(GetKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE, "my_streaming_table");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE_MONITOR_URL, "tcp://172.3.4.19:9002");
        testRunner.setProperty(GetKineticaToAvro.PROP_BATCH_SIZE, "500");
        testRunner.setProperty(GetKineticaToAvro.PROP_INCLUDE_SCHEMA, "true");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        GetKineticaToAvro processor = (GetKineticaToAvro) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(GetKineticaToAvro.REL_SUCCESS));
        assertEquals(1, processor.getRelationships().size());
    }

    @Test
    public void testAllAvroSpecificPropertiesExist() {
        GetKineticaToAvro processor = (GetKineticaToAvro) testRunner.getProcessor();

        // Verify all Avro-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(GetKineticaToAvro.PROP_BATCH_SIZE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(GetKineticaToAvro.PROP_INCLUDE_SCHEMA));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(GetKineticaToAvro.PROP_TABLE_MONITOR_URL));
    }

    @Test
    public void testTableMonitorUrlFormats() {
        testRunner.setProperty(GetKineticaToAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE, "test_table");

        // TCP URL
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE_MONITOR_URL, "tcp://localhost:9002");
        testRunner.assertValid();

        // TCP with IP
        testRunner.setProperty(GetKineticaToAvro.PROP_TABLE_MONITOR_URL, "tcp://192.168.1.100:9002");
        testRunner.assertValid();
    }
}
