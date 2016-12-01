package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.gpudb.Avro;
import com.gpudb.Type;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.protocol.CreateTableMonitorResponse;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

@Tags({"gpudb", "get"})
@CapabilityDescription("Monitors a set in GPUdb and reads new objects into CSV files")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to text/csv")
public class GetGPUdb extends AbstractProcessor {
    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name("Server URL")
            .description("URL of the GPUdb server")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SET = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("Name of the GPUdb table")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_OBJECT_MONITOR = new PropertyDescriptor.Builder()
            .name("Table Monitor URL")
            .description("URL of the GPUdb table monitor")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder()
            .name("Delimiter")
            .description("Delimiter of input data (usually a ',' or '\t' (tab); defaults to '\t' (tab))")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\t")
            .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All CSV files from the GPUdb set are routed to this relationship")
            .build();

    private GPUdb gpudb;
    private String set;
    private Type objectType;
    private Thread mainThread;
    private ConcurrentLinkedQueue<GenericRecord> queue;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private char delimiter;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_SERVER);
        descriptors.add(PROP_SET);
        descriptors.add(PROP_OBJECT_MONITOR);
        descriptors.add(PROP_DELIMITER);        
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
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

        gpudb = new GPUdb(context.getProperty(PROP_SERVER).getValue());
        set = context.getProperty(PROP_SET).getValue();
        delimiter = context.getProperty(PROP_DELIMITER).getValue().charAt(0);
        objectType = Type.fromTable(gpudb, set);
        queue = new ConcurrentLinkedQueue<>();

        mainThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    CreateTableMonitorResponse response = gpudb.createTableMonitor(set, null);

                    String topicId = response.getTopicId();

                    try (Context zmqContext = ZMQ.context(1); Socket subscriber = zmqContext.socket(ZMQ.SUB)) {
                        subscriber.connect(context.getProperty(PROP_OBJECT_MONITOR).getValue());
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
                    getLogger().error("Unable to get data from {}", new Object[] { context.getProperty(PROP_OBJECT_MONITOR).getValue() }, ex);
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

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
               try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
                CSVPrinter printer = new CSVPrinter(writer, CSVFormat.RFC4180.withDelimiter(delimiter));
                       
                ArrayList<String> fields = new ArrayList<>();

                for (Type.Column attribute : objectType.getColumns()) {
                    String field = attribute.getName() + "|";

                    if (attribute.getType() == Double.TYPE) {
                        field += "double";
                    } else if (attribute.getType() == Float.TYPE) {
                        field += "float";
                    } else if (attribute.getType() == Integer.TYPE) {
                        field += "int";
                    } else if (attribute.getType() == Long.TYPE) {
                        field += "long";
                    } else {
                        field += "string";
                    }

                    for (String annotation : attribute.getProperties()) {
                        field += "|" + annotation;
                    }

                    fields.add(field);
                }

                printer.printRecord(fields);
                int count = 0;

                for (GenericRecord object : objectList) {
                    fields.clear();

                    for (int i = 0; i < objectType.getColumns().size(); i++) {
                        fields.add(object.get(i).toString());
                    }

                    printer.printRecord(fields);
                    count++;
                }

                printer.flush();
                writer.flush();
                out.flush();
                getLogger().info("Got {} record(s) from set {} at {}.", new Object[] { count, set, gpudb.getURL() });
                
               }
            }
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".csv");
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().receive(flowFile, gpudb.getURL().toString(), set);
        session.transfer(flowFile, REL_SUCCESS);
    }
}