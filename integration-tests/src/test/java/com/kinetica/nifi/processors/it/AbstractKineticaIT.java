package com.kinetica.nifi.processors.it;

import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.ClearTableRequest;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableResponse;
import com.gpudb.protocol.ShowTableRequest;
import com.gpudb.protocol.ShowTableResponse;

import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Base class for Kinetica NiFi processor integration tests.
 *
 * <p>This class provides:
 * <ul>
 *   <li>Kinetica connection management</li>
 *   <li>Test table creation and cleanup</li>
 *   <li>Common test utilities</li>
 *   <li>Connectivity checks (skips tests if Kinetica unavailable)</li>
 * </ul>
 *
 * <p>Configuration via system properties:
 * <ul>
 *   <li>{@code kinetica.url} - Kinetica server URL (default: http://localhost:9191)</li>
 *   <li>{@code kinetica.username} - Username (optional)</li>
 *   <li>{@code kinetica.password} - Password (optional)</li>
 *   <li>{@code it.test.table.prefix} - Prefix for test tables (default: nifi_it_)</li>
 * </ul>
 *
 * <p>Run integration tests:
 * <pre>
 * mvn verify -pl integration-tests -Dkinetica.url=http://kinetica:9191
 * </pre>
 */
public abstract class AbstractKineticaIT {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractKineticaIT.class);

    // Configuration from system properties
    protected static final String KINETICA_URL = System.getProperty("kinetica.url", "http://localhost:9191");
    protected static final String KINETICA_USERNAME = System.getProperty("kinetica.username", "");
    protected static final String KINETICA_PASSWORD = System.getProperty("kinetica.password", "");
    protected static final String TABLE_PREFIX = System.getProperty("it.test.table.prefix", "nifi_it_");

    // Shared Kinetica connection
    protected static GPUdb gpudb;
    protected static boolean kineticaAvailable = false;

    // Tables created during tests (for cleanup)
    private final List<String> testTables = new ArrayList<>();

    // Current test name
    protected String testName;

    /**
     * Initialize Kinetica connection before all tests.
     */
    @BeforeAll
    static void initializeKinetica() {
        logger.info("Initializing Kinetica connection to: {}", KINETICA_URL);

        try {
            GPUdbBase.Options options = new GPUdbBase.Options();

            if (KINETICA_USERNAME != null && !KINETICA_USERNAME.isEmpty()) {
                options.setUsername(KINETICA_USERNAME);
                options.setPassword(KINETICA_PASSWORD);
            }

            // Connection settings
            options.setTimeout(30000); // 30 second timeout
            options.setThreadCount(4);

            gpudb = new GPUdb(KINETICA_URL, options);

            // Test connectivity
            gpudb.showSystemStatus(new HashMap<>());
            kineticaAvailable = true;

            logger.info("Successfully connected to Kinetica at: {}", KINETICA_URL);

        } catch (Exception e) {
            logger.warn("Failed to connect to Kinetica at {}: {}", KINETICA_URL, e.getMessage());
            logger.warn("Integration tests will be skipped. Start Kinetica or set -Dkinetica.url");
            kineticaAvailable = false;
        }
    }

    /**
     * Close Kinetica connection after all tests.
     */
    @AfterAll
    static void closeKinetica() {
        if (gpudb != null) {
            try {
                // GPUdb doesn't have an explicit close, but we can clear resources
                logger.info("Kinetica connection cleanup complete");
            } catch (Exception e) {
                logger.warn("Error during Kinetica cleanup: {}", e.getMessage());
            }
        }
    }

    /**
     * Set up before each test.
     */
    @BeforeEach
    void setUp(TestInfo testInfo) {
        // Skip test if Kinetica is not available
        assumeTrue(kineticaAvailable,
                "Kinetica is not available at " + KINETICA_URL + ". Skipping integration test.");

        testName = testInfo.getDisplayName();
        logger.info("Starting test: {}", testName);
    }

    /**
     * Clean up after each test.
     */
    @AfterEach
    void tearDown() {
        // Clean up test tables
        for (String tableName : testTables) {
            try {
                dropTableIfExists(tableName);
                logger.debug("Cleaned up test table: {}", tableName);
            } catch (Exception e) {
                logger.warn("Failed to clean up table {}: {}", tableName, e.getMessage());
            }
        }
        testTables.clear();

        logger.info("Completed test: {}", testName);
    }

    // ========== Test Table Management ==========

    /**
     * Generates a unique test table name.
     */
    protected String generateTestTableName() {
        String tableName = TABLE_PREFIX + UUID.randomUUID().toString().replace("-", "_").substring(0, 8);
        testTables.add(tableName);
        return tableName;
    }

    /**
     * Creates a test table with the specified schema.
     *
     * @param tableName Table name
     * @param type      Table type/schema
     * @return The created table name
     */
    protected String createTestTable(String tableName, Type type) throws GPUdbException {
        // Drop if exists
        dropTableIfExists(tableName);

        // Create table
        String typeId = type.create(gpudb);

        Map<String, String> options = new HashMap<>();
        gpudb.createTable(tableName, typeId, options);

        testTables.add(tableName);
        logger.info("Created test table: {} with type: {}", tableName, typeId);

        return tableName;
    }

    /**
     * Creates a simple test table with common column types.
     */
    protected String createSimpleTestTable(String tableName) throws GPUdbException {
        Type type = new Type(
                new Type.Column("id", Long.class, ColumnProperty.PRIMARY_KEY),
                new Type.Column("name", String.class),
                new Type.Column("value", Double.class),
                new Type.Column("timestamp", Long.class, ColumnProperty.TIMESTAMP)
        );
        return createTestTable(tableName, type);
    }

    /**
     * Drops a table if it exists.
     */
    protected void dropTableIfExists(String tableName) throws GPUdbException {
        if (tableExists(tableName)) {
            Map<String, String> options = new HashMap<>();
            options.put(ClearTableRequest.Options.NO_ERROR_IF_NOT_EXISTS, "true");
            gpudb.clearTable(tableName, null, options);
            logger.debug("Dropped table: {}", tableName);
        }
    }

    /**
     * Checks if a table exists.
     */
    protected boolean tableExists(String tableName) throws GPUdbException {
        HasTableResponse response = gpudb.hasTable(tableName, null);
        return response.getTableExists();
    }

    /**
     * Gets the record count for a table.
     */
    protected long getTableRecordCount(String tableName) throws GPUdbException {
        Map<String, String> options = new HashMap<>();
        options.put(ShowTableRequest.Options.GET_SIZES, "true");

        ShowTableResponse response = gpudb.showTable(tableName, options);
        return response.getTotalSize();
    }

    // ========== NiFi Test Runner Helpers ==========

    /**
     * Configures a TestRunner with Kinetica connection properties.
     */
    protected void configureKineticaConnection(TestRunner runner, String tableName) {
        runner.setProperty("Server URL", KINETICA_URL);
        runner.setProperty("Table Name", tableName);

        if (KINETICA_USERNAME != null && !KINETICA_USERNAME.isEmpty()) {
            runner.setProperty("Username", KINETICA_USERNAME);
            runner.setProperty("Password", KINETICA_PASSWORD);
        }
    }

    /**
     * Asserts that a table has the expected record count.
     */
    protected void assertTableRecordCount(String tableName, long expectedCount) throws GPUdbException {
        long actualCount = getTableRecordCount(tableName);
        if (actualCount != expectedCount) {
            fail("Expected " + expectedCount + " records in table " + tableName +
                    ", but found " + actualCount);
        }
    }

    // ========== Test Data Helpers ==========

    /**
     * Generates test CSV content.
     */
    protected String generateTestCsvData(int recordCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("id,name,value,timestamp\n");

        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            sb.append(i).append(",");
            sb.append("name_").append(i).append(",");
            sb.append(i * 1.5).append(",");
            sb.append(baseTimestamp + i * 1000).append("\n");
        }

        return sb.toString();
    }

    /**
     * Generates test JSON array content.
     */
    protected String generateTestJsonArrayData(int recordCount) {
        StringBuilder sb = new StringBuilder();
        sb.append("[\n");

        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            if (i > 0) sb.append(",\n");
            sb.append("  {");
            sb.append("\"id\":").append(i).append(",");
            sb.append("\"name\":\"name_").append(i).append("\",");
            sb.append("\"value\":").append(i * 1.5).append(",");
            sb.append("\"timestamp\":").append(baseTimestamp + i * 1000);
            sb.append("}");
        }

        sb.append("\n]");
        return sb.toString();
    }

    /**
     * Generates test NDJSON content.
     */
    protected String generateTestNdjsonData(int recordCount) {
        StringBuilder sb = new StringBuilder();

        long baseTimestamp = System.currentTimeMillis();
        for (int i = 0; i < recordCount; i++) {
            sb.append("{");
            sb.append("\"id\":").append(i).append(",");
            sb.append("\"name\":\"name_").append(i).append("\",");
            sb.append("\"value\":").append(i * 1.5).append(",");
            sb.append("\"timestamp\":").append(baseTimestamp + i * 1000);
            sb.append("}\n");
        }

        return sb.toString();
    }

    /**
     * Waits for a condition to be true with timeout.
     */
    protected void waitForCondition(long timeoutMs, long pollIntervalMs, ConditionChecker checker)
            throws Exception {
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            if (checker.check()) {
                return;
            }
            Thread.sleep(pollIntervalMs);
        }

        fail("Condition not met within " + timeoutMs + "ms");
    }

    @FunctionalInterface
    protected interface ConditionChecker {
        boolean check() throws Exception;
    }
}
