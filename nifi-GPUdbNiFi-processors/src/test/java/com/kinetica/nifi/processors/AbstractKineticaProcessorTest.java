package com.kinetica.nifi.processors;

import com.kinetica.nifi.processors.base.AbstractKineticaProcessor;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Unit tests for AbstractKineticaProcessor base class.
 * Tests property validation and configuration handling.
 */
public class AbstractKineticaProcessorTest {

    private TestRunner testRunner;

    /**
     * Concrete implementation for testing the abstract class.
     */
    public static class TestableKineticaProcessor extends AbstractKineticaProcessor {

        public static final Relationship REL_SUCCESS = new Relationship.Builder()
                .name("success")
                .description("Success relationship")
                .build();

        public static final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .description("Failure relationship")
                .build();

        private List<PropertyDescriptor> descriptors;
        private Set<Relationship> relationships;

        @Override
        protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
            this.descriptors = Collections.unmodifiableList(getBasePropertyDescriptors());
            Set<Relationship> rels = new HashSet<>();
            rels.add(REL_SUCCESS);
            rels.add(REL_FAILURE);
            this.relationships = Collections.unmodifiableSet(rels);
        }

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
            return descriptors;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            // No-op for testing
        }
    }

    @Before
    public void setUp() {
        testRunner = TestRunners.newTestRunner(TestableKineticaProcessor.class);
    }

    @Test
    public void testRequiredProperties() {
        // Server URL and Table Name are required
        testRunner.assertNotValid();

        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.assertNotValid(); // Still missing table name

        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");
        testRunner.assertValid();
    }

    @Test
    public void testValidTableNames() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");

        // Simple table name
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "my_table");
        testRunner.assertValid();

        // Table name with schema
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "schema.my_table");
        testRunner.assertValid();

        // Table name starting with letter
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "Table123");
        testRunner.assertValid();
    }

    @Test
    public void testOptionalCredentials() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // Without credentials - should be valid
        testRunner.assertValid();

        // With credentials - should still be valid
        testRunner.setProperty(AbstractKineticaProcessor.PROP_USERNAME, "admin");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_PASSWORD, "password123");
        testRunner.assertValid();
    }

    @Test
    public void testServerUrlFormats() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // HTTP URL
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.assertValid();

        // HTTPS URL
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "https://kinetica.example.com:8443/gpudb-0");
        testRunner.assertValid();

        // URL with path
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://10.0.0.1:9191/gpudb-0");
        testRunner.assertValid();
    }

    // ========== SSL/TLS Tests ==========

    @Test
    public void testSSLProperties() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "https://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // SSL enabled
        testRunner.setProperty(AbstractKineticaProcessor.PROP_USE_SSL, "true");
        testRunner.assertValid();

        // Bypass cert check (for self-signed certs)
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SSL_BYPASS_CERT_CHECK, "true");
        testRunner.assertValid();
    }

    @Test
    public void testSSLDisabled() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_USE_SSL, "false");
        testRunner.assertValid();
    }

    // ========== Connection Timeout Tests ==========

    @Test
    public void testConnectionTimeoutProperties() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // Custom connection timeout
        testRunner.setProperty(AbstractKineticaProcessor.PROP_CONNECTION_TIMEOUT, "45 sec");
        testRunner.assertValid();

        // Custom socket timeout
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SOCKET_TIMEOUT, "120 sec");
        testRunner.assertValid();
    }

    @Test
    public void testConnectionPoolSizeProperty() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // Custom pool size
        testRunner.setProperty(AbstractKineticaProcessor.PROP_CONNECTION_POOL_SIZE, "8");
        testRunner.assertValid();
    }

    @Test
    public void testProcessorHasExpectedRelationships() {
        TestableKineticaProcessor processor = (TestableKineticaProcessor) testRunner.getProcessor();

        // Should have success and failure relationships
        assertTrue(processor.getRelationships().contains(TestableKineticaProcessor.REL_SUCCESS));
        assertTrue(processor.getRelationships().contains(TestableKineticaProcessor.REL_FAILURE));
        assertEquals(2, processor.getRelationships().size());
    }

    @Test
    public void testProcessorHasExpectedProperties() {
        TestableKineticaProcessor processor = (TestableKineticaProcessor) testRunner.getProcessor();

        // Should have the base properties
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_SERVER));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_TABLE));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_USERNAME));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_PASSWORD));
        // Should have SSL properties
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_USE_SSL));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_SSL_BYPASS_CERT_CHECK));
        // Should have connection properties
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_CONNECTION_TIMEOUT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_SOCKET_TIMEOUT));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_CONNECTION_POOL_SIZE));
        // Should have cluster control properties
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_DISABLE_AUTO_DISCOVERY));
        assertTrue(processor.getSupportedPropertyDescriptors().contains(AbstractKineticaProcessor.PROP_DISABLE_FAILOVER));
    }

    // ========== Disable Auto Discovery Tests ==========

    @Test
    public void testDisableAutoDiscoveryProperty() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // Default (false) - should be valid
        testRunner.assertValid();

        // Explicitly enable auto discovery (default behavior)
        testRunner.setProperty(AbstractKineticaProcessor.PROP_DISABLE_AUTO_DISCOVERY, "false");
        testRunner.assertValid();

        // Disable auto discovery (useful with load balancers)
        testRunner.setProperty(AbstractKineticaProcessor.PROP_DISABLE_AUTO_DISCOVERY, "true");
        testRunner.assertValid();
    }

    @Test
    public void testDisableFailoverProperty() {
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://localhost:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");

        // Default (false) - should be valid
        testRunner.assertValid();

        // Explicitly enable failover (default behavior)
        testRunner.setProperty(AbstractKineticaProcessor.PROP_DISABLE_FAILOVER, "false");
        testRunner.assertValid();

        // Disable failover (useful with load balancers)
        testRunner.setProperty(AbstractKineticaProcessor.PROP_DISABLE_FAILOVER, "true");
        testRunner.assertValid();
    }

    @Test
    public void testLoadBalancerConfiguration() {
        // Typical load balancer configuration - disable both auto discovery and failover
        testRunner.setProperty(AbstractKineticaProcessor.PROP_SERVER, "http://loadbalancer.example.com:9191");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_TABLE, "test_table");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_DISABLE_AUTO_DISCOVERY, "true");
        testRunner.setProperty(AbstractKineticaProcessor.PROP_DISABLE_FAILOVER, "true");

        testRunner.assertValid();
    }
}
