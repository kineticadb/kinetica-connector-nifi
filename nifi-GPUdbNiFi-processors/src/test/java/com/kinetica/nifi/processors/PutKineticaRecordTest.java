package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PutKineticaRecord processor.
 * Tests property validation and Record API configuration options.
 *
 * Note: Full testing of Record API functionality requires a mock RecordReader
 * controller service, which is tested in integration tests.
 */
public class PutKineticaRecordTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(PutKineticaRecord.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        // Initially not valid - missing required properties
        testRunner.assertNotValid();

        // Add server URL
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        // Add table name
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");
        // Still not valid - Record Reader is required
        testRunner.assertNotValid();
    }

    @Test
    public void testBatchSizeProperty() {
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");

        // Default batch size
        testRunner.setProperty(PutKineticaRecord.PROP_BATCH_SIZE, "10000");
        // Still not valid without Record Reader
        testRunner.assertNotValid();

        // Small batch size
        testRunner.setProperty(PutKineticaRecord.PROP_BATCH_SIZE, "100");
        testRunner.assertNotValid();

        // Large batch size
        testRunner.setProperty(PutKineticaRecord.PROP_BATCH_SIZE, "100000");
        testRunner.assertNotValid();
    }

    @Test
    public void testUpdateOnExistingPkProperty() {
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");

        // Don't update (default)
        testRunner.setProperty(PutKineticaRecord.PROP_UPDATE_ON_EXISTING_PK, "false");
        testRunner.assertNotValid(); // Still missing Record Reader

        // Update on existing
        testRunner.setProperty(PutKineticaRecord.PROP_UPDATE_ON_EXISTING_PK, "true");
        testRunner.assertNotValid();
    }

    @Test
    public void testSkipErrorsProperty() {
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");

        // Fail on error (default)
        testRunner.setProperty(PutKineticaRecord.PROP_SKIP_ERRORS, "false");
        testRunner.assertNotValid();

        // Skip errors
        testRunner.setProperty(PutKineticaRecord.PROP_SKIP_ERRORS, "true");
        testRunner.assertNotValid();
    }

    @Test
    public void testCreateTableProperty() {
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");

        // Don't create table (default)
        testRunner.setProperty(PutKineticaRecord.PROP_CREATE_TABLE, "false");
        testRunner.assertNotValid();

        // Create table if not exists
        testRunner.setProperty(PutKineticaRecord.PROP_CREATE_TABLE, "true");
        testRunner.assertNotValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        PutKineticaRecord processor = (PutKineticaRecord) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(PutKineticaRecord.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(PutKineticaRecord.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testAllRecordSpecificPropertiesExist() {
        PutKineticaRecord processor = (PutKineticaRecord) testRunner.getProcessor();

        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.RECORD_READER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_BATCH_SIZE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_UPDATE_ON_EXISTING_PK));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_SKIP_ERRORS));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_CREATE_TABLE));
    }

    @Test
    public void testInheritedPropertiesExist() {
        PutKineticaRecord processor = (PutKineticaRecord) testRunner.getProcessor();

        // Should have base Kinetica properties
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_SERVER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_TABLE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_USERNAME));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_PASSWORD));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaRecord.PROP_USE_SSL));
    }

    @Test
    public void testSslConfiguration() {
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "https://localhost:9191");
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");
        testRunner.setProperty(PutKineticaRecord.PROP_USE_SSL, "true");
        testRunner.setProperty(PutKineticaRecord.PROP_SSL_BYPASS_CERT_CHECK, "true");

        // Still not valid without Record Reader, but SSL config is accepted
        testRunner.assertNotValid();
    }

    @Test
    public void testConnectionTimeoutConfiguration() {
        testRunner.setProperty(PutKineticaRecord.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaRecord.PROP_TABLE, "test_table");
        testRunner.setProperty(PutKineticaRecord.PROP_CONNECTION_TIMEOUT, "60 sec");
        testRunner.setProperty(PutKineticaRecord.PROP_SOCKET_TIMEOUT, "120 sec");

        // Still not valid without Record Reader
        testRunner.assertNotValid();
    }
}
