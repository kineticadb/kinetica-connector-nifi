package com.gisfederal.gpudb.processors.GPUdbNiFi;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for PutKinetica processor property validation.
 * These tests do NOT require a live Kinetica instance.
 */
public class TestPutKineticaProperties {

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(PutKinetica.class);
    }

    @Test
    public void testValidPropertiesPass() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertValid();
    }

    @Test
    public void testMissingServerUrlFails() {
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testMissingTableNameFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testInvalidServerUrlFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "not-a-url");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testInvalidBatchSizeFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "-1");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testZeroBatchSizeFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "0");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testNonNumericBatchSizeFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "abc");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }

    @Test
    public void testOptionalUsernamePasswordValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.setProperty(KineticaConstants.USERNAME, "admin");
        runner.setProperty(KineticaConstants.PASSWORD, "secret");
        runner.assertValid();
    }

    @Test
    public void testOptionalSchemaValid() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.setProperty(KineticaConstants.SCHEMA, "x|Float|data,y|Float|data");
        runner.assertValid();
    }

    @Test
    public void testInvalidUpdateOnExistingPkFails() {
        runner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        runner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        runner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "not-a-boolean");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        runner.assertNotValid();
    }
}
