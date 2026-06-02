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

    // ========== Additional Property Tests ==========

    @Test
    public void testCollectionNameProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Collection name is optional
        testRunner.assertValid();

        // With collection name
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_COLLECTION, "my_collection");
        testRunner.assertValid();
    }

    @Test
    public void testReplicateTableProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Replicate table (default is false)
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_REPLICATE_TABLE, "false");
        testRunner.assertValid();

        // Enable table replication
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_REPLICATE_TABLE, "true");
        testRunner.assertValid();
    }

    @Test
    public void testAvroSchemaProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Avro schema as alternative to Schema Definition
        String avroSchema = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"TestRecord\"," +
                "\"fields\": [" +
                "  {\"name\": \"id\", \"type\": \"long\"}," +
                "  {\"name\": \"name\", \"type\": \"string\"}" +
                "]" +
                "}";

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA, avroSchema);
        testRunner.assertValid();
    }

    @Test
    public void testSchemaWithTimestampAnnotation() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Schema with timestamp column
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA,
                "id|Long|data|primary_key,event_time|Long|data|timestamp,message|String|data");
        testRunner.assertValid();
    }

    @Test
    public void testSchemaWithStoreOnlyAnnotation() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Schema with store_only (not indexed) column
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA,
                "id|Long|data|primary_key,content|String|store_only");
        testRunner.assertValid();
    }

    @Test
    public void testSchemaWithTextSearchAnnotation() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Schema with text_search enabled
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA,
                "id|Long|data|primary_key,title|String|data|text_search,content|String|store_only|text_search");
        testRunner.assertValid();
    }

    @Test
    public void testAllSupportedColumnTypes() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Schema with all supported types
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_SCHEMA,
                "id|Long|data|primary_key," +
                "int_col|Integer|data," +
                "float_col|Float|data," +
                "double_col|Double|data," +
                "string_col|String|data");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasExpectedProperties() {
        PutKinetica processor = (PutKinetica) testRunner.getProcessor();

        // Should have Put-specific properties
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_SCHEMA));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_BATCH_SIZE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_COLLECTION));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_UPDATE_ON_EXISTING_PK));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_REPLICATE_TABLE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_DATE_FORMAT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_TIMEZONE));
    }

    @Test
    public void testSSLProperties() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "https://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Enable SSL
        testRunner.setProperty(PutKinetica.PROP_USE_SSL, "true");
        testRunner.assertValid();

        // Bypass cert check for self-signed certs
        testRunner.setProperty(PutKinetica.PROP_SSL_BYPASS_CERT_CHECK, "true");
        testRunner.assertValid();
    }

    @Test
    public void testConnectionTimeoutProperties() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Custom connection timeout
        testRunner.setProperty(PutKinetica.PROP_CONNECTION_TIMEOUT, "60 sec");
        testRunner.assertValid();

        // Custom socket timeout
        testRunner.setProperty(PutKinetica.PROP_SOCKET_TIMEOUT, "120 sec");
        testRunner.assertValid();
    }

    @Test
    public void testDisableAutoDiscoveryProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Disable auto discovery (useful with load balancers)
        testRunner.setProperty(PutKinetica.PROP_DISABLE_AUTO_DISCOVERY, "true");
        testRunner.assertValid();
    }

    @Test
    public void testDisableFailoverProperty() {
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");

        // Disable failover (useful with load balancers)
        testRunner.setProperty(PutKinetica.PROP_DISABLE_FAILOVER, "true");
        testRunner.assertValid();
    }

    @Test
    public void testFullConfigurationForLoadBalancer() {
        // Complete configuration for load balancer scenario
        testRunner.setProperty(PutKinetica.PROP_SERVER, "http://loadbalancer.example.com:9191");
        testRunner.setProperty(PutKinetica.PROP_TABLE, "test_table");
        testRunner.setProperty(PutKinetica.PROP_USERNAME, "admin");
        testRunner.setProperty(PutKinetica.PROP_PASSWORD, "password");
        testRunner.setProperty(PutKinetica.PROP_DISABLE_AUTO_DISCOVERY, "true");
        testRunner.setProperty(PutKinetica.PROP_DISABLE_FAILOVER, "true");
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_BATCH_SIZE, "10000");
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_UPDATE_ON_EXISTING_PK, "true");

        testRunner.assertValid();
    }
}
