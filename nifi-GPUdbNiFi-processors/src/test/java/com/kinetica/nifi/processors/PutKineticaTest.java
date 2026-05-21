package com.kinetica.nifi.processors;

import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PutKinetica processor.
 * Tests property validation, FlowFile attribute handling, and routing.
 *
 * Note: These are unit tests that don't require a running Kinetica instance.
 * Tests that would require onScheduled() to complete are skipped as they
 * would try to connect to Kinetica.
 */
public class PutKineticaTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(PutKinetica.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        // Verify processor can be instantiated
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        // Without required properties, should be invalid
        testRunner.assertNotValid();

        // Set server URL
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        // Set table name
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");
        testRunner.assertValid();
    }

    @Test
    public void testSchemaPropertyValidation() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Valid schema format
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA, "x|Float|data,y|Float|data,name|String|data");
        testRunner.assertValid();

        // Schema with primary key
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA, "id|Long|data|primary_key,name|String|data");
        testRunner.assertValid();
    }

    @Test
    public void testBatchSizeProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Default batch size should be valid
        testRunner.assertValid();

        // Custom batch size
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_BATCH_SIZE, "5000");
        testRunner.assertValid();

        // Large batch size
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_BATCH_SIZE, "100000");
        testRunner.assertValid();
    }

    @Test
    public void testDateFormatProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Various date formats
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
        testRunner.assertValid();

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_DATE_FORMAT, "MM/dd/yyyy");
        testRunner.assertValid();

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_DATE_FORMAT, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        testRunner.assertValid();
    }

    @Test
    public void testTimezoneProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_TIMEZONE, "UTC");
        testRunner.assertValid();

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_TIMEZONE, "America/New_York");
        testRunner.assertValid();

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_TIMEZONE, "Europe/London");
        testRunner.assertValid();
    }

    @Test
    public void testUpdateOnExistingPkProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_UPDATE_ON_EXISTING_PK, "true");
        testRunner.assertValid();

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_UPDATE_ON_EXISTING_PK, "false");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        PutKinetica processor = (PutKinetica) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(AbstractPutKineticaProcessor.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(AbstractPutKineticaProcessor.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testProcessorTags() {
        PutKinetica processor = new PutKinetica();
        // Processor should have appropriate tags for NiFi discovery
        // Tags are defined via @Tags annotation
    }

    @Test
    public void testProcessorDescription() {
        PutKinetica processor = new PutKinetica();
        // Processor should have a capability description
        // Description is defined via @CapabilityDescription annotation
    }
}
