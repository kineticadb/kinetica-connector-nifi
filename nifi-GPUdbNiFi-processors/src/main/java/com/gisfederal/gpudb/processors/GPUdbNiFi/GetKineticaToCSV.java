package com.gisfederal.gpudb.processors.GPUdbNiFi;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
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

@Tags({"gpudb", "get"})
@CapabilityDescription("Monitors a set in GPUdb and reads new objects into CSV files")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to text/csv")
public class GetKineticaToCSV extends AbstractProcessor {
    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name( KineticaConstants.SERVER_URL )
            .description("URL of the GPUdb server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SET = new PropertyDescriptor.Builder()
            .name( KineticaConstants.TABLE_NAME )
            .description("Name of the GPUdb table")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_OBJECT_MONITOR = new PropertyDescriptor.Builder()
            .name( KineticaConstants.TABLE_MONITOR_URL )
            .description("URL of the GPUdb table monitor")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder()
            .name( KineticaConstants.DELIMITER )
            .description("Delimiter of input data (usually a ',' or '\t' (tab); defaults to '\t' (tab))")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\t")
            .build();
    
    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name( KineticaConstants.USERNAME )
            .description("Username to connect to Kinetica")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build(); 
    
    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name( KineticaConstants.PASSWORD )
            .description("Password to connect to Kinetica")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PROP_DISABLE_AUTO_DISCOVERY = new PropertyDescriptor.Builder()
            .name(KineticaConstants.DISABLE_AUTO_DISCOVERY)
            .description("Disable automatic cluster discovery. Set to true when connecting through a proxy or load balancer where internal cluster IPs are not reachable.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();

    public static final PropertyDescriptor PROP_DISABLE_FAILOVER = new PropertyDescriptor.Builder()
            .name(KineticaConstants.DISABLE_FAILOVER)
            .description("Disable automatic failover to other cluster nodes. Set to true when using a single-endpoint proxy.")
            .required(false)
            .defaultValue("false")
            .allowableValues("true", "false")
            .build();
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name( KineticaConstants.SUCCESS )
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
        descriptors.add(PROP_USERNAME);
        descriptors.add(PROP_PASSWORD);
        descriptors.add(PROP_DISABLE_AUTO_DISCOVERY);
        descriptors.add(PROP_DISABLE_FAILOVER);
        
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

        Options option = new Options();
        if (context.getProperty(PROP_USERNAME).evaluateAttributeExpressions().getValue() != null && context.getProperty(PROP_PASSWORD).getValue() != null) {
            option.setUsername(context.getProperty(PROP_USERNAME).evaluateAttributeExpressions().getValue());
            option.setPassword(context.getProperty(PROP_PASSWORD).getValue());
        }
        if (context.getProperty(PROP_DISABLE_AUTO_DISCOVERY).asBoolean()) {
            option.setDisableAutoDiscovery(true);
        }
        if (context.getProperty(PROP_DISABLE_FAILOVER).asBoolean()) {
            option.setDisableFailover(true);
        }
        gpudb = new GPUdb(context.getProperty(PROP_SERVER).evaluateAttributeExpressions().getValue(), option);
        
        set = context.getProperty(PROP_SET).evaluateAttributeExpressions().getValue();
        delimiter = context.getProperty(PROP_DELIMITER).evaluateAttributeExpressions().getValue().charAt(0);
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
                    getLogger().error("Unable to get data from {}", new Object[] { context.getProperty(PROP_OBJECT_MONITOR).evaluateAttributeExpressions().getValue() }, ex);
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
                CSVPrinter printer = new CSVPrinter(writer, CSVFormat.RFC4180.builder().setDelimiter(delimiter).build());
                       
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
                printer.close();
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
