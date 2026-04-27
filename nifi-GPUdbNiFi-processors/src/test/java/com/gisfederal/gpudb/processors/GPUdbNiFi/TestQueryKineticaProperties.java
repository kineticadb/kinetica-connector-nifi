package com.gisfederal.gpudb.processors.GPUdbNiFi;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for QueryKineticaToCSV, QueryKineticaToJSON, and QueryKineticaToAvro
 * processor property validation. These do NOT require a live Kinetica instance.
 */
public class TestQueryKineticaProperties {

    private TestRunner csvRunner;
    private TestRunner jsonRunner;
    private TestRunner avroRunner;

    @Before
    public void setup() {
        csvRunner = TestRunners.newTestRunner(QueryKineticaToCSV.class);
        jsonRunner = TestRunners.newTestRunner(QueryKineticaToJSON.class);
        avroRunner = TestRunners.newTestRunner(QueryKineticaToAvro.class);
    }

    // ─── QueryKineticaToCSV ───────────────────────────────────────────

    @Test
    public void testCsvValidWithTable() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        csvRunner.setProperty(KineticaConstants.DELIMITER, ",");
        csvRunner.assertValid();
    }

    @Test
    public void testCsvValidWithSql() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.SQL_QUERY, "SELECT * FROM demo.test");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        csvRunner.setProperty(KineticaConstants.DELIMITER, "|");
        csvRunner.assertValid();
    }

    @Test
    public void testCsvInvalidBothTableAndSql() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        csvRunner.setProperty(KineticaConstants.SQL_QUERY, "SELECT * FROM demo.test");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        csvRunner.setProperty(KineticaConstants.DELIMITER, ",");
        csvRunner.assertNotValid();
    }

    @Test
    public void testCsvInvalidNeitherTableNorSql() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        csvRunner.setProperty(KineticaConstants.DELIMITER, ",");
        csvRunner.assertNotValid();
    }

    @Test
    public void testCsvMissingServerUrlFails() {
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        csvRunner.setProperty(KineticaConstants.DELIMITER, ",");
        csvRunner.assertNotValid();
    }

    @Test
    public void testCsvInvalidBatchSizeFails() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "-1");
        csvRunner.setProperty(KineticaConstants.DELIMITER, ",");
        csvRunner.assertNotValid();
    }

    @Test
    public void testCsvWithCredentialsValid() {
        csvRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        csvRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        csvRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        csvRunner.setProperty(KineticaConstants.DELIMITER, ",");
        csvRunner.setProperty(KineticaConstants.USERNAME, "admin");
        csvRunner.setProperty(KineticaConstants.PASSWORD, "secret");
        csvRunner.assertValid();
    }

    // ─── QueryKineticaToJSON ──────────────────────────────────────────

    @Test
    public void testJsonValidWithTable() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        jsonRunner.assertValid();
    }

    @Test
    public void testJsonValidWithSql() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.SQL_QUERY, "SELECT id, name FROM demo.test WHERE id > 0");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        jsonRunner.assertValid();
    }

    @Test
    public void testJsonInvalidBothTableAndSql() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test");
        jsonRunner.setProperty(KineticaConstants.SQL_QUERY, "SELECT * FROM demo.test");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonInvalidNeitherTableNorSql() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonMissingServerUrlFails() {
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        jsonRunner.assertNotValid();
    }

    @Test
    public void testJsonWithCredentialsValid() {
        jsonRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        jsonRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test");
        jsonRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        jsonRunner.setProperty(KineticaConstants.USERNAME, "admin");
        jsonRunner.setProperty(KineticaConstants.PASSWORD, "secret");
        jsonRunner.assertValid();
    }

    // ─── QueryKineticaToAvro ──────────────────────────────────────────

    @Test
    public void testAvroValidWithTable() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        avroRunner.assertValid();
    }

    @Test
    public void testAvroValidWithSql() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.SQL_QUERY, "SELECT * FROM demo.test");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "500");
        avroRunner.assertValid();
    }

    @Test
    public void testAvroInvalidBothTableAndSql() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test");
        avroRunner.setProperty(KineticaConstants.SQL_QUERY, "SELECT * FROM demo.test");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        avroRunner.assertNotValid();
    }

    @Test
    public void testAvroInvalidNeitherTableNorSql() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        avroRunner.assertNotValid();
    }

    @Test
    public void testAvroMissingServerUrlFails() {
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test_table");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        avroRunner.assertNotValid();
    }

    @Test
    public void testAvroWithCredentialsValid() {
        avroRunner.setProperty(KineticaConstants.SERVER_URL, "http://localhost:9191");
        avroRunner.setProperty(KineticaConstants.TABLE_NAME, "demo.test");
        avroRunner.setProperty(KineticaConstants.BATCH_SIZE, "1000");
        avroRunner.setProperty(KineticaConstants.USERNAME, "admin");
        avroRunner.setProperty(KineticaConstants.PASSWORD, "secret");
        avroRunner.assertValid();
    }
}
