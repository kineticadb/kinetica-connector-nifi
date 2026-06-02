package com.kinetica.nifi.processors.base;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.gpudb.Avro;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.CreateTableMonitorResponse;

/**
 * Abstract base class for Kinetica Get processors (GetKineticaToCSV, GetKineticaToJSON, etc.).
 *
 * <p>This class extends AbstractKineticaProcessor with functionality specific to
 * reading data from Kinetica via table monitors:
 * <ul>
 *   <li>ZeroMQ table monitor integration</li>
 *   <li>Asynchronous record queuing</li>
 *   <li>Proper resource cleanup (connection leaks fixed)</li>
 *   <li>Thread-safe monitor management</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
public abstract class AbstractGetKineticaProcessor extends AbstractKineticaProcessor {

    // ========== GET-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_TABLE_MONITOR_URL = new PropertyDescriptor.Builder()
            .name("Table Monitor URL")
            .displayName("Table Monitor URL")
            .description("URL of the Kinetica table monitor endpoint for ZeroMQ subscriptions. " +
                    "Example: tcp://172.3.4.19:9002")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // ========== RELATIONSHIPS ==========

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles containing data read from Kinetica")
            .build();

    // ========== SHARED STATE ==========

    protected volatile Type objectType;
    protected volatile ConcurrentLinkedQueue<GenericRecord> recordQueue;
    protected volatile String tableMonitorUrl;

    // Thread management
    private volatile Thread monitorThread;
    private volatile String topicId;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // ZeroMQ resources (need explicit cleanup)
    private volatile Context zmqContext;
    private volatile Socket zmqSubscriber;

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    // ========== INITIALIZATION ==========

    @Override
    protected void init(org.apache.nifi.processor.ProcessorInitializationContext context) {
        // Build property descriptors
        List<PropertyDescriptor> props = new ArrayList<>();
        props.addAll(getBasePropertyDescriptors());
        props.add(PROP_TABLE_MONITOR_URL);
        // Allow subclasses to add more properties
        props.addAll(getAdditionalPropertyDescriptors());
        this.descriptors = Collections.unmodifiableList(props);

        // Build relationships
        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);
    }

    /**
     * Override this method in subclasses to add additional property descriptors.
     *
     * @return List of additional property descriptors (can be empty)
     */
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        return Collections.emptyList();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    // ========== LIFECYCLE ==========

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        tableMonitorUrl = context.getProperty(PROP_TABLE_MONITOR_URL).getValue();
        recordQueue = new ConcurrentLinkedQueue<>();

        try {
            // Get the table type
            objectType = Type.fromTable(gpudb, tableName);

            // Start the monitor thread
            startMonitorThread(context);

        } catch (GPUdbException e) {
            throw new ProcessException("Failed to initialize Get processor: " + e.getMessage(), e);
        }
    }

    @Override
    @OnStopped
    public void onStopped() {
        // Stop the monitor thread first
        stopMonitorThread();

        // Clean up ZeroMQ resources
        cleanupZmqResources();

        // Clear the record queue
        if (recordQueue != null) {
            recordQueue.clear();
            recordQueue = null;
        }

        objectType = null;

        super.onStopped();
    }

    // ========== MONITOR THREAD MANAGEMENT ==========

    /**
     * Starts the table monitor thread that listens for new records via ZeroMQ.
     */
    private void startMonitorThread(final ProcessContext context) throws GPUdbException {
        running.set(true);

        // Create the table monitor
        CreateTableMonitorResponse response = gpudb.createTableMonitor(tableName, null);
        topicId = response.getTopicId();

        getLogger().info("Created table monitor for '{}' with topic ID: {}", tableName, topicId);

        monitorThread = new Thread(() -> {
            try {
                runMonitorLoop(context);
            } catch (Exception e) {
                if (running.get()) {
                    getLogger().error("Monitor thread error: {}", e.getMessage(), e);
                }
            }
        }, "Kinetica-Monitor-" + tableName);

        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    /**
     * Main loop for the monitor thread.
     */
    private void runMonitorLoop(ProcessContext context) {
        try {
            // Create ZeroMQ context and socket
            zmqContext = ZMQ.context(1);
            zmqSubscriber = zmqContext.socket(ZMQ.SUB);

            zmqSubscriber.connect(tableMonitorUrl);
            zmqSubscriber.subscribe(topicId.getBytes());
            zmqSubscriber.setReceiveTimeOut(1000); // 1 second timeout for graceful shutdown

            getLogger().debug("Connected to table monitor at {}", tableMonitorUrl);

            while (running.get() && !Thread.currentThread().isInterrupted()) {
                ZMsg message = ZMsg.recvMsg(zmqSubscriber, false);

                if (message == null) {
                    continue;
                }

                try {
                    processMessage(message);
                } finally {
                    message.destroy();
                }
            }

        } catch (Exception e) {
            if (running.get()) {
                getLogger().error("Error in monitor loop: {}", e.getMessage(), e);
            }
        } finally {
            // Clean up the table monitor
            clearTableMonitor();
        }
    }

    /**
     * Processes a ZeroMQ message containing Avro-encoded records.
     */
    private void processMessage(ZMsg message) {
        boolean skipFirst = true;

        for (ZFrame frame : message) {
            // First frame is the topic ID, skip it
            if (skipFirst) {
                skipFirst = false;
                continue;
            }

            try {
                GenericRecord record = Avro.decode(
                        objectType.getSchema(),
                        ByteBuffer.wrap(frame.getData())
                );
                recordQueue.add(record);
            } catch (Exception e) {
                getLogger().warn("Failed to decode record: {}", e.getMessage());
            }
        }
    }

    /**
     * Stops the monitor thread gracefully.
     */
    private void stopMonitorThread() {
        running.set(false);

        if (monitorThread != null) {
            monitorThread.interrupt();
            try {
                monitorThread.join(5000); // Wait up to 5 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            monitorThread = null;
        }
    }

    /**
     * Clears the table monitor in Kinetica.
     */
    private void clearTableMonitor() {
        if (topicId != null && gpudb != null) {
            try {
                gpudb.clearTableMonitor(topicId, null);
                getLogger().debug("Cleared table monitor with topic ID: {}", topicId);
            } catch (Exception e) {
                getLogger().warn("Failed to clear table monitor: {}", e.getMessage());
            }
            topicId = null;
        }
    }

    /**
     * Cleans up ZeroMQ resources to prevent connection leaks.
     * This is a CRITICAL fix for the connection leak issue.
     */
    private void cleanupZmqResources() {
        if (zmqSubscriber != null) {
            try {
                zmqSubscriber.close();
            } catch (Exception e) {
                getLogger().warn("Error closing ZMQ subscriber: {}", e.getMessage());
            }
            zmqSubscriber = null;
        }

        if (zmqContext != null) {
            try {
                zmqContext.close();
            } catch (Exception e) {
                getLogger().warn("Error closing ZMQ context: {}", e.getMessage());
            }
            zmqContext = null;
        }
    }

    // ========== RECORD ACCESS ==========

    /**
     * Retrieves all queued records and clears the queue.
     *
     * @return List of records that were queued
     */
    protected List<GenericRecord> drainRecordQueue() {
        List<GenericRecord> records = new ArrayList<>();

        if (recordQueue != null) {
            GenericRecord record;
            while ((record = recordQueue.poll()) != null) {
                records.add(record);
            }
        }

        return records;
    }

    /**
     * Returns the object type for the monitored table.
     *
     * @return The Type object
     */
    protected Type getObjectType() {
        return objectType;
    }
}
