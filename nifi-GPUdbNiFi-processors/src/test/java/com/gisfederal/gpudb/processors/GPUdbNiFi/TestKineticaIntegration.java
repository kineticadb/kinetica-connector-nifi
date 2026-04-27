package com.gisfederal.gpudb.processors.GPUdbNiFi;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase.Options;
import com.gpudb.Type;
import com.gpudb.protocol.ClearTableRequest;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Integration tests for Kinetica NiFi processors.
 * These tests require a live Kinetica instance.
 *
 * Set environment variables (or .env file in project root):
 *   KINETICA_JDBC_URL  - Kinetica server URL (e.g. http://host:9191)
 *   KINETICA_USERNAME  - Kinetica username
 *   KINETICA_PASSWORD  - Kinetica password
 *   KINETICA_SCHEMA    - Optional schema name
 *
 * Tests are automatically skipped if KINETICA_JDBC_URL is not set.
 */
public class TestKineticaIntegration {

    private static final Logger LOG = LoggerFactory.getLogger(TestKineticaIntegration.class);

    private static String serverUrl;
    private static String username;
    private static String password;
    private static String schema;
    private static GPUdb gpudb;

    private TestRunner runner;
    private String testTableName;

    @BeforeClass
    public static void initClass() throws Exception {
        loadEnvFile();

        serverUrl = getEnvOrProperty("KINETICA_JDBC_URL");
        username = getEnvOrProperty("KINETICA_USERNAME");
        password = getEnvOrProperty("KINETICA_PASSWORD");
        schema = getEnvOrProperty("KINETICA_SCHEMA");

        // Skip all tests if no Kinetica URL is configured
        assumeTrue("KINETICA_JDBC_URL not set — skipping integration tests",
                   serverUrl != null && !serverUrl.isEmpty());

        // Extract the base HTTP URL from JDBC URL if needed
        String httpUrl = serverUrl;
        if (httpUrl.startsWith("jdbc:kinetica:")) {
            // Parse URL= parameter from JDBC URL
            String[] parts = httpUrl.substring("jdbc:kinetica:".length()).split(";");
            for (String part : parts) {
                if (part.startsWith("URL=")) {
                    httpUrl = part.substring(4);
                    break;
                }
            }
        }

        Options opts = new Options();
        if (username != null && password != null) {
            opts.setUsername(username);
            opts.setPassword(password);
        }
        gpudb = new GPUdb(httpUrl, opts);
        LOG.info("Connected to Kinetica at: {}", gpudb.getURL());
    }

    @Before
    public void setup() {
        testTableName = "nifi_test_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        if (schema != null && !schema.isEmpty()) {
            testTableName = schema + "." + testTableName;
        }
    }

    @After
    public void cleanup() {
        if (gpudb != null && testTableName != null) {
            try {
                ClearTableRequest req = new ClearTableRequest();
                req.setTableName(testTableName);
                req.setOptions(GPUdb.options(
                    ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS,
                    ClearTableRequest.Options.TRUE));
                gpudb.clearTable(req);
                LOG.info("Cleaned up test table: {}", testTableName);
            } catch (Exception e) {
                LOG.warn("Failed to clean up test table {}: {}", testTableName, e.getMessage());
            }
        }
    }

    @Test
    public void testPutKineticaFromFileInsertAndVerify() throws Exception {
        // Create a test table
        Type type = new Type(
            new Type.Column("id", Integer.class),
            new Type.Column("name", String.class),
            new Type.Column("value", Double.class)
        );
        String typeId = type.create(gpudb);
        gpudb.createTable(testTableName, typeId, null);
        LOG.info("Created test table: {}", testTableName);

        // Set up the processor
        runner = TestRunners.newTestRunner(PutKineticaFromFile.class);
        runner.setProperty(KineticaConstants.SERVER_URL, gpudb.getURL().toString());
        runner.setProperty(KineticaConstants.TABLE_NAME, testTableName);
        runner.setProperty(KineticaConstants.BATCH_SIZE, "100");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        if (username != null) {
            runner.setProperty(KineticaConstants.USERNAME, username);
        }
        if (password != null) {
            runner.setProperty(KineticaConstants.PASSWORD, password);
        }

        // Enqueue CSV data
        String csvData = "id,name,value\n1,alpha,1.1\n2,beta,2.2\n3,gamma,3.3\n";
        InputStream content = new ByteArrayInputStream(csvData.getBytes(StandardCharsets.UTF_8));
        runner.enqueue(content);

        // Run
        runner.run();
        runner.assertQueueEmpty();

        // Verify rows were inserted
        Map<String, String> showOpts = GPUdb.options(
            ShowTableRequest.Options.GET_SIZES,
            ShowTableRequest.Options.TRUE);
        ShowTableResponse response = gpudb.showTable(testTableName, showOpts);
        long rowCount = response.getTotalSize();
        LOG.info("Table {} has {} rows", testTableName, rowCount);
        assertEquals("Expected 3 rows inserted", 3, rowCount);

        // Verify success relationship
        List<MockFlowFile> successes = runner.getFlowFilesForRelationship(KineticaConstants.SUCCESS);
        assertFalse("Expected at least one success FlowFile", successes.isEmpty());
    }

    @Test
    public void testPutKineticaFromFileBadRecordsToFailure() throws Exception {
        // Create a test table with integer columns
        Type type = new Type(
            new Type.Column("x", Integer.class),
            new Type.Column("y", Integer.class)
        );
        String typeId = type.create(gpudb);
        gpudb.createTable(testTableName, typeId, null);

        runner = TestRunners.newTestRunner(PutKineticaFromFile.class);
        runner.setProperty(KineticaConstants.SERVER_URL, gpudb.getURL().toString());
        runner.setProperty(KineticaConstants.TABLE_NAME, testTableName);
        runner.setProperty(KineticaConstants.BATCH_SIZE, "100");
        runner.setProperty(KineticaConstants.DELIMITER, ",");
        runner.setProperty(KineticaConstants.SKIP_ERRORS, "true");
        runner.setProperty(KineticaConstants.UPDATE_ON_EXISTING_PK, "false");
        runner.setProperty(KineticaConstants.REPLICATE_TABLE, "false");
        if (username != null) {
            runner.setProperty(KineticaConstants.USERNAME, username);
        }
        if (password != null) {
            runner.setProperty(KineticaConstants.PASSWORD, password);
        }

        // CSV with some bad rows (empty values for non-nullable int columns)
        String csvData = "x,y\n1,1\n,2\n3,3\n";
        runner.enqueue(csvData.getBytes(StandardCharsets.UTF_8));
        runner.run();
        runner.assertQueueEmpty();

        // Should have some failures
        List<MockFlowFile> failures = runner.getFlowFilesForRelationship(KineticaConstants.FAILURE);
        assertFalse("Expected failure FlowFile for bad records", failures.isEmpty());
    }

    /**
     * Loads environment variables from a .env file in the project root.
     */
    private static final Map<String, String> envFileVars = new HashMap<>();

    private static void loadEnvFile() {
        File envFile = new File(".env");
        if (!envFile.exists()) {
            // Try project root (parent of module directory)
            envFile = new File("..", ".env");
        }
        if (!envFile.exists()) {
            envFile = new File(System.getProperty("user.dir"), ".env");
        }
        if (!envFile.exists()) {
            // Try parent of user.dir (module may be a child)
            File parentDir = new File(System.getProperty("user.dir")).getParentFile();
            if (parentDir != null) {
                envFile = new File(parentDir, ".env");
            }
        }
        if (envFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(envFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("#")) continue;
                    int eq = line.indexOf('=');
                    if (eq > 0) {
                        String key = line.substring(0, eq).trim();
                        String value = line.substring(eq + 1).trim();
                        envFileVars.put(key, value);
                    }
                }
            } catch (Exception e) {
                LOG.warn("Failed to read .env file: {}", e.getMessage());
            }
        }
    }

    private static String getEnvOrProperty(String key) {
        String val = System.getenv(key);
        if (val == null || val.isEmpty()) {
            val = System.getProperty(key);
        }
        if (val == null || val.isEmpty()) {
            val = envFileVars.get(key);
        }
        return val;
    }
}
