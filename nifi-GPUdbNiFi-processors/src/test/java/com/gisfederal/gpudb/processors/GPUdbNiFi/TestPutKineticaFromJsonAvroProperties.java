package com.gisfederal.gpudb.processors.GPUdbNiFi;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for PutKineticaFromJSON and PutKineticaFromAvro processor
 * property validation. These do NOT require a live Kinetica instance.
 */
public class TestPutKineticaFromJsonAvroProperties {

    private TestRunner jsonRunner;
    private TestRunner avroRunner;

    @Before
    public void setup() {
        jsonRunner = TestRunners.newTestRunner(PutKineticaFromJSON.class);
        avroRunner = TestRunners.newTestRunner(PutKineticaFromAvro.class);
    }

    // ─── PutKineticaFromJSON ──────────────────────────────────────────

    @Test
    public void testJsonValidProperties() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        jsonRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        jsonRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        jsonRunner.assertValid();
    }

    @Test
    public void testJsonMissingServerUrlFails() {
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        jsonRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        jsonRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonMissingTableNameFails() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        jsonRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        jsonRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonWithCredentials() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        jsonRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        jsonRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        jsonRunner.setProperty(KineticaConstants.USERNAME, "admin");
        jsonRunner.setProperty(KineticaConstants.PASSWORD, "pass123");
        jsonRunner.assertValid();
    }

    @Test
    public void testJsonInvalidBatchSizeFails() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "0");
        jsonRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        jsonRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        jsonRunner.assertNotValid();
    }

    // ─── PutKineticaFromAvro ──────────────────────────────────────────

    @Test
    public void testAvroValidProperties() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        avroRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        avroRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        avroRunner.assertValid();
    }

    @Test
    public void testAvroMissingServerUrlFails() {
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        avroRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        avroRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        avroRunner.assertNotValid();
    }

    @Test
    public void testAvroMissingTableNameFails() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        avroRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        avroRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        avroRunner.assertNotValid();
    }

    @Test
    public void testAvroWithCredentials() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        avroRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        avroRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        avroRunner.setProperty(KineticaConstants.USERNAME, "admin");
        avroRunner.setProperty(KineticaConstants.PASSWORD, "pass123");
        avroRunner.assertValid();
    }

    @Test
    public void testAvroInvalidBatchSizeFails() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "test_table");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "-5");
        avroRunner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        avroRunner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        avroRunner.assertNotValid();
    }
}
