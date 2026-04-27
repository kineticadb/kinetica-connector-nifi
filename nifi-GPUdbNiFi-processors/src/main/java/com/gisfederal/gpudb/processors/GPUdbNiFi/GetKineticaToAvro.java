package com.gisfederal.gpudb.processors.GPUdbNiFi;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import com.gpudb.Avro;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase.Options;
import com.gpudb.GPUdbException;
import com.gpudb.Type;
import com.gpudb.protocol.CreateTableMonitorResponse;

@Tags({"Kinetica", "gpudb", "get", "avro"})
@CapabilityDescription("Monitors a Kinetica table via ZeroMQ table monitor and outputs new records as "
        + "Apache Avro container files (application/avro-binary). Each FlowFile contains one or more "
        + "Avro records using the table's schema. Requires a running table monitor endpoint.")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to application/avro-binary")
public class GetKineticaToAvro extends AbstractProcessor {

    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name(KineticaConstants.SERVER_URL)
            .description("URL of the Kinetica server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SET = new PropertyDescriptor.Builder()
            .name(KineticaConstants.TABLE_NAME)
            .description("Name of the Kinetica table to monitor")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_OBJECT_MONITOR = new PropertyDescriptor.Builder()
            .name(KineticaConstants.TABLE_MONITOR_URL)
            .description("URL of the Kinetica ZeroMQ table monitor endpoint")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name(KineticaConstants.USERNAME)
            .description("Username to connect to Kinetica")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name(KineticaConstants.PASSWORD)
            .description("Password to connect to Kinetica")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name(KineticaConstants.SUCCESS)
            .description("All Avro files from the Kinetica table are routed to this relationship")
            .build();

    private GPUdb gpudb;
    private String set;
    private Type objectType;
    private Thread mainThread;
    private ConcurrentLinkedQueue<GenericRecord> queue;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptorsList = new ArrayList<>();
        descriptorsList.add(PROP_SERVER);
        descriptorsList.add(PROP_SET);
        descriptorsList.add(PROP_OBJECT_MONITOR);
        descriptorsList.add(PROP_USERNAME);
        descriptorsList.add(PROP_PASSWORD);

        this.descriptors = Collections.unmodifiableList(descriptorsList);

        final Set<Relationship> relationshipsList = new HashSet<>();
        relationshipsList.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationshipsList);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws GPUdbException {
        Options option = new Options();
        if (context.getProperty(PROP_USERNAME).evaluateAttributeExpressions().getValue() != null
                && context.getProperty(PROP_PASSWORD).getValue() != null) {
            option.setUsername(context.getProperty(PROP_USERNAME).evaluateAttributeExpressions().getValue());
            option.setPassword(context.getProperty(PROP_PASSWORD).getValue());
        }
        gpudb = new GPUdb(context.getProperty(PROP_SERVER).evaluateAttributeExpressions().getValue(), option);

        set = context.getProperty(PROP_SET).evaluateAttributeExpressions().getValue();
        objectType = Type.fromTable(gpudb, set);
        queue = new ConcurrentLinkedQueue<>();

        mainThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    CreateTableMonitorResponse response = gpudb.createTableMonitor(set, null);
                    String topicId = response.getTopicId();

                    try (ZContext zmqContext = new ZContext(1)) {
                        ZMQ.Socket subscriber = zmqContext.createSocket(SocketType.SUB);
                        subscriber.connect(context.getProperty(PROP_OBJECT_MONITOR).evaluateAttributeExpressions().getValue());
                        subscriber.subscribe(topicId.getBytes());
                        subscriber.setReceiveTimeOut(1000);

                        while (!Thread.currentThread().isInterrupted()) {
                            ZMsg message = ZMsg.recvMsg(subscriber);

                            if (message == null) {
                                continue;
                            }

                            boolean skip = true;

                            for (ZFrame frame : message) {
                                if (skip) {
                                    skip = false;
                                    continue;
                                }

                                GenericRecord object = Avro.decode(objectType.getSchema(), ByteBuffer.wrap(frame.getData()));
                                queue.add(object);
                            }
                        }

                        gpudb.clearTableMonitor(topicId, null);
                    }
                } catch (Exception ex) {
                    getLogger().error("Unable to get data from {}",
                            new Object[]{context.getProperty(PROP_OBJECT_MONITOR).evaluateAttributeExpressions().getValue()}, ex);
                }
            }
        });
        mainThread.start();
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        if (mainThread != null) {
            mainThread.interrupt();
            mainThread = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<GenericRecord> objectList = new ArrayList<>();

        while (true) {
            GenericRecord object = queue.poll();

            if (object == null) {
                break;
            }

            objectList.add(object);
        }

        if (objectList.isEmpty()) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        final Schema schema = objectType.getSchema();

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
                try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
                    dataFileWriter.create(schema, out);
                    for (GenericRecord record : objectList) {
                        dataFileWriter.append(record);
                    }
                } catch (Exception ex) {
                    getLogger().error("Error writing Avro output for table {}", new Object[]{set}, ex);
                }
            }
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/avro-binary");
        attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".avro");
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().receive(flowFile, gpudb.getURL().toString(), set);
        session.transfer(flowFile, REL_SUCCESS);

        getLogger().info("Got {} Avro record(s) from table {} at {}.",
                new Object[]{objectList.size(), set, gpudb.getURL()});
    }
}
