package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for ListKineticaTables processor.
 * Tests property validation and configuration options.
 */
public class ListKineticaTablesTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(ListKineticaTables.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        // Server URL is required
        testRunner.assertNotValid();

        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");
        // Table pattern has a default value of "*"
        testRunner.assertValid();
    }

    @Test
    public void testTablePatternProperty() {
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");

        // Default pattern - all tables
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_PATTERN, "*");
        testRunner.assertValid();

        // Schema-specific pattern
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_PATTERN, "my_schema");
        testRunner.assertValid();

        // Wildcard pattern
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_PATTERN, "my_schema.sales_*");
        testRunner.assertValid();
    }

    @Test
    public void testIncludeSizesProperty() {
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");

        // Include sizes (default)
        testRunner.setProperty(ListKineticaTables.PROP_INCLUDE_SIZES, "true");
        testRunner.assertValid();

        // Exclude sizes
        testRunner.setProperty(ListKineticaTables.PROP_INCLUDE_SIZES, "false");
        testRunner.assertValid();
    }

    @Test
    public void testIncludeChildrenProperty() {
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");

        // Include children (default)
        testRunner.setProperty(ListKineticaTables.PROP_INCLUDE_CHILDREN, "true");
        testRunner.assertValid();

        // Exclude children
        testRunner.setProperty(ListKineticaTables.PROP_INCLUDE_CHILDREN, "false");
        testRunner.assertValid();
    }

    @Test
    public void testTableTypeFilterProperty() {
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");

        // Empty filter (all types) - use default which is valid
        testRunner.assertValid();

        // Filter by TABLE
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_TYPE_FILTER, "TABLE");
        testRunner.assertValid();

        // Filter by VIEW
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_TYPE_FILTER, "VIEW");
        testRunner.assertValid();

        // Filter by COLLECTION
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_TYPE_FILTER, "COLLECTION");
        testRunner.assertValid();

        // Filter by MATERIALIZED_VIEW
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_TYPE_FILTER, "MATERIALIZED_VIEW");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        ListKineticaTables processor = (ListKineticaTables) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(ListKineticaTables.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(ListKineticaTables.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testAllListSpecificPropertiesExist() {
        ListKineticaTables processor = (ListKineticaTables) testRunner.getProcessor();

        assertTrue(processor.getSupportedPropertyDescriptors().contains(ListKineticaTables.PROP_TABLE_PATTERN));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ListKineticaTables.PROP_INCLUDE_SIZES));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ListKineticaTables.PROP_INCLUDE_CHILDREN));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ListKineticaTables.PROP_TABLE_TYPE_FILTER));
    }

    @Test
    public void testTypicalConfiguration() {
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(ListKineticaTables.PROP_USERNAME, "admin");
        testRunner.setProperty(ListKineticaTables.PROP_PASSWORD, "password");
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_PATTERN, "production.*");
        testRunner.setProperty(ListKineticaTables.PROP_INCLUDE_SIZES, "true");
        testRunner.setProperty(ListKineticaTables.PROP_INCLUDE_CHILDREN, "false");
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_TYPE_FILTER, "TABLE");

        testRunner.assertValid();
    }

    @Test
    public void testDoesNotRequireTableName() {
        // ListKineticaTables does NOT require PROP_TABLE - it uses PROP_TABLE_PATTERN instead
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(ListKineticaTables.PROP_TABLE_PATTERN, "*");

        // Should be valid without setting Table Name property
        testRunner.assertValid();
    }

    @Test
    public void testSslConfiguration() {
        testRunner.setProperty(ListKineticaTables.PROP_SERVER, "https://localhost:9191");
        testRunner.setProperty(ListKineticaTables.PROP_USE_SSL, "true");
        testRunner.setProperty(ListKineticaTables.PROP_SSL_BYPASS_CERT_CHECK, "true");

        testRunner.assertValid();
    }
}
