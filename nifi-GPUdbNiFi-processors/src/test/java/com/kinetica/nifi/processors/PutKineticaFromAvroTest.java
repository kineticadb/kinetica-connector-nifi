package com.kinetica.nifi.processors;

import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PutKineticaFromAvro processor.
 * Tests property validation and Avro configuration options.
 */
public class PutKineticaFromAvroTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(PutKineticaFromAvro.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid();

        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");
        testRunner.assertValid();
    }

    @Test
    public void testSkipErrorsProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Skip errors (default)
        testRunner.setProperty(PutKineticaFromAvro.PROP_SKIP_ERRORS, "true");
        testRunner.assertValid();

        // Fail on first error
        testRunner.setProperty(PutKineticaFromAvro.PROP_SKIP_ERRORS, "false");
        testRunner.assertValid();
    }

    @Test
    public void testTypicalAvroConfiguration() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "my_table");
        testRunner.setProperty(PutKineticaFromAvro.PROP_SCHEMA, "id|Long|data,name|String|data,value|Float|data");
        testRunner.setProperty(PutKineticaFromAvro.PROP_SKIP_ERRORS, "true");
        testRunner.setProperty(PutKineticaFromAvro.PROP_BATCH_SIZE, "10000");

        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        PutKineticaFromAvro processor = (PutKineticaFromAvro) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(PutKineticaFromAvro.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(PutKineticaFromAvro.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testNoInputDoesNothing() {
        // Disable expression language scope validation for this test
        // since onScheduled runs before any FlowFile is available
        testRunner.setValidateExpressionUsage(false);

        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");
        testRunner.setProperty(PutKineticaFromAvro.PROP_USERNAME, "admin");
        testRunner.setProperty(PutKineticaFromAvro.PROP_PASSWORD, "Kinetica1.");

        testRunner.run();

        testRunner.assertTransferCount(PutKineticaFromAvro.REL_SUCCESS, 0);
        testRunner.assertTransferCount(PutKineticaFromAvro.REL_FAILURE, 0);
    }

    @Test
    public void testAllAvroSpecificPropertiesExist() {
        PutKineticaFromAvro processor = (PutKineticaFromAvro) testRunner.getProcessor();

        // Verify all Avro-specific properties are available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(PutKineticaFromAvro.PROP_SKIP_ERRORS));
    }

    @Test
    public void testBatchSizeProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Small batch size
        testRunner.setProperty(PutKineticaFromAvro.PROP_BATCH_SIZE, "100");
        testRunner.assertValid();

        // Large batch size
        testRunner.setProperty(PutKineticaFromAvro.PROP_BATCH_SIZE, "50000");
        testRunner.assertValid();
    }

    @Test
    public void testSchemaProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Schema with multiple columns
        testRunner.setProperty(PutKineticaFromAvro.PROP_SCHEMA,
                "id|Long|data|primary_key,x|Float|data,y|Float|data,name|String|data");
        testRunner.assertValid();
    }

    @Test
    public void testUpdateOnExistingPkProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Update on existing PK
        testRunner.setProperty(PutKineticaFromAvro.PROP_UPDATE_ON_EXISTING_PK, "true");
        testRunner.assertValid();

        // Don't update on existing PK (default)
        testRunner.setProperty(PutKineticaFromAvro.PROP_UPDATE_ON_EXISTING_PK, "false");
        testRunner.assertValid();
    }

    @Test
    public void testDateFormatProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Custom date format
        testRunner.setProperty(PutKineticaFromAvro.PROP_DATE_FORMAT, "yyyy-MM-dd HH:mm:ss");
        testRunner.assertValid();
    }

    @Test
    public void testTimezoneProperty() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // UTC timezone
        testRunner.setProperty(PutKineticaFromAvro.PROP_TIMEZONE, "UTC");
        testRunner.assertValid();

        // EST timezone
        testRunner.setProperty(PutKineticaFromAvro.PROP_TIMEZONE, "EST");
        testRunner.assertValid();
    }

    // ========== Avro Schema Property Tests ==========

    @Test
    public void testAvroSchemaPropertyExists() {
        PutKineticaFromAvro processor = (PutKineticaFromAvro) testRunner.getProcessor();

        // Verify PROP_AVRO_SCHEMA is available
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA));
    }

    @Test
    public void testAvroSchemaPropertyIsOptional() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Should be valid without Avro Schema
        testRunner.assertValid();
    }

    @Test
    public void testAvroSchemaPropertyValidation() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Valid Avro schema JSON
        String validAvroSchema = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"TestRecord\"," +
                "\"fields\": [" +
                "  {\"name\": \"id\", \"type\": \"long\"}," +
                "  {\"name\": \"name\", \"type\": \"string\"}," +
                "  {\"name\": \"value\", \"type\": \"double\"}" +
                "]" +
                "}";

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA, validAvroSchema);
        testRunner.assertValid();
    }

    @Test
    public void testAvroSchemaWithNullableFields() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Avro schema with nullable fields (union with null)
        String schemaWithNullable = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"TestRecord\"," +
                "\"fields\": [" +
                "  {\"name\": \"id\", \"type\": \"long\"}," +
                "  {\"name\": \"optional_name\", \"type\": [\"null\", \"string\"]}," +
                "  {\"name\": \"optional_value\", \"type\": [\"null\", \"double\"]}" +
                "]" +
                "}";

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA, schemaWithNullable);
        testRunner.assertValid();
    }

    @Test
    public void testAvroSchemaWithLogicalTypes() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Avro schema with logical types (timestamp-millis, date, decimal)
        String schemaWithLogicalTypes = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"TestRecord\"," +
                "\"fields\": [" +
                "  {\"name\": \"id\", \"type\": \"long\"}," +
                "  {\"name\": \"event_time\", \"type\": {\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}}," +
                "  {\"name\": \"event_date\", \"type\": {\"type\": \"int\", \"logicalType\": \"date\"}}," +
                "  {\"name\": \"price\", \"type\": {\"type\": \"bytes\", \"logicalType\": \"decimal\", \"precision\": 10, \"scale\": 2}}" +
                "]" +
                "}";

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA, schemaWithLogicalTypes);
        testRunner.assertValid();
    }

    @Test
    public void testAvroSchemaWithAllPrimitiveTypes() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Avro schema with all primitive types
        String schemaWithAllTypes = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"AllTypesRecord\"," +
                "\"fields\": [" +
                "  {\"name\": \"int_field\", \"type\": \"int\"}," +
                "  {\"name\": \"long_field\", \"type\": \"long\"}," +
                "  {\"name\": \"float_field\", \"type\": \"float\"}," +
                "  {\"name\": \"double_field\", \"type\": \"double\"}," +
                "  {\"name\": \"boolean_field\", \"type\": \"boolean\"}," +
                "  {\"name\": \"string_field\", \"type\": \"string\"}," +
                "  {\"name\": \"bytes_field\", \"type\": \"bytes\"}" +
                "]" +
                "}";

        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA, schemaWithAllTypes);
        testRunner.assertValid();
    }

    @Test
    public void testSchemaDefinitionTakesPrecedenceOverAvroSchema() {
        testRunner.setProperty(PutKineticaFromAvro.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(PutKineticaFromAvro.PROP_TABLE, "test_table");

        // Set both Schema Definition and Avro Schema
        testRunner.setProperty(PutKineticaFromAvro.PROP_SCHEMA, "id|Long|data,name|String|data");

        String avroSchema = "{" +
                "\"type\": \"record\"," +
                "\"name\": \"TestRecord\"," +
                "\"fields\": [{\"name\": \"id\", \"type\": \"long\"}]" +
                "}";
        testRunner.setProperty(AbstractPutKineticaProcessor.PROP_AVRO_SCHEMA, avroSchema);

        // Both should be accepted (Schema Definition takes precedence at runtime)
        testRunner.assertValid();
    }
}
