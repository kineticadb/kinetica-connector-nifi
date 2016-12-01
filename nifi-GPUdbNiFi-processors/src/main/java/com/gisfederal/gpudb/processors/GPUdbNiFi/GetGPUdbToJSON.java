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
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;


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
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to application/json")
public class GetGPUdbToJSON extends AbstractProcessor {
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
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptorsList = new ArrayList<>();
        descriptorsList.add(PROP_SERVER);
        descriptorsList.add(PROP_SET);
        descriptorsList.add(PROP_OBJECT_MONITOR);       
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

        gpudb = new GPUdb(context.getProperty(PROP_SERVER).getValue());
        set = context.getProperty(PROP_SET).getValue();
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
               //should output one flowfile per record, not sure about this yet
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {    
                    for (GenericRecord object : objectList) {
                        Schema schema = object.getSchema();
                        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out);
                        DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
                        datumWriter.write(object, encoder);
                        writer.flush();
                        out.flush();
                        encoder.flush();
                        getLogger().info("writing record {} to set {} at {}.", new Object[] { object.toString(), set, gpudb.getURL() });
                    }
                }
                catch(Exception ex){
                    getLogger().error("Unable to get data from {}", new Object[] { context.getProperty(PROP_OBJECT_MONITOR).getValue() }, ex);
                }
            }
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().receive(flowFile, gpudb.getURL().toString(), set);
        session.transfer(flowFile, REL_SUCCESS);
    }
}