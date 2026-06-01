package com.kinetica.nifi.processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for ExecuteKineticaSQL processor.
 * Tests property validation and SQL execution configuration options.
 */
public class ExecuteKineticaSQLTest {

    private TestRunner testRunner;

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(ExecuteKineticaSQL.class);
    }

    @Test
    public void testProcessorIsConfigurable() {
        assertNotNull(testRunner.getProcessor());
    }

    @Test
    public void testRequiredPropertiesValidation() {
        // Server URL is required
        testRunner.assertNotValid();

        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");
        // With SQL Source=Property Value, SQL Statement is needed for valid config
        // However, processor allows input without SQL if using FlowFile content
        testRunner.assertValid();
    }

    @Test
    public void testSqlSourceProperty() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");

        // Property Value (default)
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_PROPERTY);
        testRunner.assertValid();

        // FlowFile Content
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_CONTENT);
        testRunner.assertValid();

        // FlowFile Attribute
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_ATTRIBUTE);
        testRunner.assertValid();
    }

    @Test
    public void testSqlStatementProperty() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_PROPERTY);

        // DDL statement
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT,
                "CREATE TABLE test_table (id INT, name VARCHAR(100))");
        testRunner.assertValid();

        // DML statement
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT,
                "INSERT INTO test_table VALUES (1, 'test')");
        testRunner.assertValid();

        // UPDATE statement
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT,
                "UPDATE test_table SET name = 'updated' WHERE id = 1");
        testRunner.assertValid();

        // DELETE statement
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT,
                "DELETE FROM test_table WHERE id = 1");
        testRunner.assertValid();
    }

    @Test
    public void testExecutionTimeoutProperty() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");

        // Default timeout
        testRunner.setProperty(ExecuteKineticaSQL.PROP_EXECUTION_TIMEOUT, "300");
        testRunner.assertValid();

        // Short timeout
        testRunner.setProperty(ExecuteKineticaSQL.PROP_EXECUTION_TIMEOUT, "30");
        testRunner.assertValid();

        // No timeout
        testRunner.setProperty(ExecuteKineticaSQL.PROP_EXECUTION_TIMEOUT, "0");
        testRunner.assertValid();
    }

    @Test
    public void testFailOnErrorProperty() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");

        // Fail on error (default)
        testRunner.setProperty(ExecuteKineticaSQL.PROP_FAIL_ON_ERROR, "true");
        testRunner.assertValid();

        // Continue on error
        testRunner.setProperty(ExecuteKineticaSQL.PROP_FAIL_ON_ERROR, "false");
        testRunner.assertValid();
    }

    @Test
    public void testReturnResultsProperty() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");

        // Don't return results (default)
        testRunner.setProperty(ExecuteKineticaSQL.PROP_RETURN_RESULTS, "false");
        testRunner.assertValid();

        // Return results
        testRunner.setProperty(ExecuteKineticaSQL.PROP_RETURN_RESULTS, "true");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasCorrectRelationships() {
        ExecuteKineticaSQL processor = (ExecuteKineticaSQL) testRunner.getProcessor();

        assertTrue(processor.getRelationships().contains(ExecuteKineticaSQL.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(ExecuteKineticaSQL.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testAllSqlSpecificPropertiesExist() {
        ExecuteKineticaSQL processor = (ExecuteKineticaSQL) testRunner.getProcessor();

        assertTrue(processor.getSupportedPropertyDescriptors().contains(ExecuteKineticaSQL.PROP_SQL_SOURCE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ExecuteKineticaSQL.PROP_SQL_STATEMENT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ExecuteKineticaSQL.PROP_EXECUTION_TIMEOUT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ExecuteKineticaSQL.PROP_FAIL_ON_ERROR));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(ExecuteKineticaSQL.PROP_RETURN_RESULTS));
    }

    @Test
    public void testTypicalDdlConfiguration() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_USERNAME, "admin");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_PASSWORD, "password");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_PROPERTY);
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT,
                "CREATE TABLE orders (id BIGINT PRIMARY KEY, customer_id BIGINT, amount DOUBLE, created_at TIMESTAMP)");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_FAIL_ON_ERROR, "true");

        testRunner.assertValid();
    }

    @Test
    public void testFlowFileContentConfiguration() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_CONTENT);
        testRunner.setProperty(ExecuteKineticaSQL.PROP_FAIL_ON_ERROR, "false");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_RETURN_RESULTS, "true");

        testRunner.assertValid();
    }

    @Test
    public void testExpressionLanguageInSql() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_SOURCE, ExecuteKineticaSQL.SQL_SOURCE_PROPERTY);
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT,
                "INSERT INTO ${table.name} VALUES (${record.id}, '${record.name}')");

        testRunner.assertValid();
    }

    @Test
    public void testSslConfiguration() {
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SERVER, "https://localhost:9191");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_USE_SSL, "true");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SSL_BYPASS_CERT_CHECK, "false");
        testRunner.setProperty(ExecuteKineticaSQL.PROP_SQL_STATEMENT, "SELECT 1");

        testRunner.assertValid();
    }
}
