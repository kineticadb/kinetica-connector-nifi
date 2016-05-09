package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.InputStreamCallback;

@Tags({"gpudb", "add", "bulkadd", "put"})
@CapabilityDescription("Writes the contents of a CSV file to GPUdb")
@ReadsAttribute(attribute = "mime.type", description = "Determines MIME type of input file")
public class PutGPUdb extends AbstractProcessor {
    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name("Server URL")
            .description("URL of the GPUdb server")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_PARENT_SET = new PropertyDescriptor.Builder()
            .name("Collection Name")
            .description("Name of the GPUdb collection")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SET = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("Name of the GPUdb table")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SCHEMA = new PropertyDescriptor.Builder()
            .name("Schema")
            .description("Schema of the GPUdb table")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_LABEL = new PropertyDescriptor.Builder()
            .name("Label")
            .description("Type label of the GPUdb table")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder()
            .name("Delimiter")
            .description("Delimiter of input data (usually a ',' or '\t' (tab); defaults to '\t' (tab))")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\t")
            .build();

    public static final PropertyDescriptor PROP_SEMANTIC_TYPE = new PropertyDescriptor.Builder()
            .name("Semantic Type")
            .description("Semantic type of the GPUdb table")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_VERSION = new PropertyDescriptor.Builder()
            .name("Version")
            .description("Semantic type of the GPUdb table")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are written to GPUdb are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be written to GPUdb are routed to this relationship")
            .build();

    private GPUdb gpudb;
    private String set;
    public  Type objectType;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private ProcessorLog logger;
    private char delimiter;


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_SERVER);
        descriptors.add(PROP_PARENT_SET);
        descriptors.add(PROP_SET);
        descriptors.add(PROP_SCHEMA);
        descriptors.add(PROP_LABEL);
        descriptors.add(PROP_DELIMITER);
        descriptors.add(PROP_SEMANTIC_TYPE);
        descriptors.add(PROP_VERSION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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

    private Type createSet(ProcessContext context, CSVRecord header) throws GPUdbException {
        List<Column> attributes = new ArrayList<>();
        int maxPrimaryKey = -1;

        for (int i = 0; i < header.size(); i++) {
            String field = header.get(i);
            String[] split = field.split("\\|", -1);
            String name = split[0];
            Class<?> type;
            getLogger().info("field name:" + name + ", type:" + split[1].toLowerCase());
            if (split.length > 1) {
                switch (split[1].toLowerCase()) {
                    case "double":
                        type = Double.class;
                        break;

                    case "float":
                        type = Float.class;
                        break;

                    case "int":
                        type = Integer.class;
                        break;

                    case "long":
                        type = Long.class;
                        break;

                    case "string":
                        type = String.class;
                        break;

                    default:
                        throw new GPUdbException("Invalid data type \"" + split[1] + "\" for attribute " + name + ".");
                }
            } else {
                type = String.class;
            }

            int primaryKey = -1;
            List<String> annotations = new ArrayList<>();

            for (int j = 2; j < split.length; j++) {
                String annotation = split[j].toLowerCase();

                if (annotation.startsWith("$primary_key")) {
                    int openIndex = annotation.indexOf('(');
                    int closeIndex = annotation.indexOf(')', openIndex);
                    int keyIndex = -1;

                    if (openIndex != -1 && closeIndex != -1) {
                        try {
                            keyIndex = Integer.parseInt(annotation.substring(openIndex + 1, closeIndex));
                        } catch (NumberFormatException ex) {
                        }
                    }

                    if (keyIndex != -1) {
                        primaryKey = keyIndex;
                        maxPrimaryKey = Math.max(primaryKey, maxPrimaryKey);
                    } else {
                        primaryKey = ++maxPrimaryKey;
                    }
                } else {
                    annotations.add(annotation);
                }
            }

            attributes.add(new Column(name, type, annotations));
        }
        Type type = new Type(context.getProperty(PROP_LABEL).isSet() ? context.getProperty(PROP_LABEL).getValue() : "",attributes);


        String typeId = type.create(gpudb);
        HasTableResponse response = gpudb.hasTable(set, null);
        Map<String, String> create_table_options = null;
        String parent = context.getProperty(PROP_PARENT_SET).getValue();
        if(parent==null){
            parent = "";
        }

        create_table_options = GPUdb.options( CreateTableRequest.Options.COLLECTION_NAME,parent);

        if(!response.getTableExists()){
            gpudb.createTable(context.getProperty(PROP_SET).getValue(), typeId, create_table_options);
        }

        gpudb.addKnownType(typeId, RecordObject.class);

        return type;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws GPUdbException {
        gpudb = new GPUdb(context.getProperty(PROP_SERVER).getValue());
        set = context.getProperty(PROP_SET).getValue();
        delimiter = context.getProperty(PROP_DELIMITER).getValue().charAt(0);
       
        HasTableResponse response;

        try {
            response = gpudb.hasTable(set, null);
        } catch (GPUdbException ex) {
            getLogger().info("failed hasTable, exception:" + ex.getMessage());
            response = null;
        }

        if ((response != null) &&(response.getTableExists())){
            getLogger().info("getting type from table:" + set);
            objectType = Type.fromTable(gpudb, set);
            getLogger().info("objectType:" + objectType.toString());
        } else if (context.getProperty(PROP_SCHEMA).isSet()) {
            CSVRecord header;

            try {
                header = CSVParser.parse(context.getProperty(PROP_SCHEMA).getValue(), CSVFormat.RFC4180.withDelimiter(delimiter)).getRecords().get(0);
            } catch (Exception ex) {
                throw new GPUdbException("Invalid schema format: " + ex.getMessage(), ex);
            }
            objectType = createSet(context, header);
        } else {
            objectType = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        if (!flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()).equals("text/csv")) {
            getLogger().error("Unsupported MIME type \"{}\".", new Object[] { flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()) });
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Note: The following are length 1 arrays so that they can be declared
        // final and they can be used in anonymous functions.

        final Type[] type = new Type[1];
        final int[][] attributeNumbers = new int[1][];
        final boolean[] failed = { false };

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) {
                try {
                    CSVParser parser = new CSVParser(new InputStreamReader(in), CSVFormat.RFC4180.withDelimiter(delimiter));
                    CSVRecord header = parser.iterator().next();   
                    synchronized (PutGPUdb.this) {
                        type[0] = objectType;

                        if (type[0] == null) {
                            type[0] = createSet(context, header);
                            objectType = type[0];
                        }
                    }
                    attributeNumbers[0] = new int[header.size()];
                    int attributeCount = 0;

                    for (int i = 0; i < header.size(); i++) {
                        String field = header.get(i);
                        int pipe = field.indexOf('|');

                        if (pipe > -1) {
                            field = field.substring(0, pipe);
                        }
                        else{
                            getLogger().error("didn't find pipe");
                        }

                        attributeNumbers[0][i] = -1;
                        for (int j = 0; j < type[0].getColumns().size(); j++) {
                            if (type[0].getColumns().get(j).getName().equalsIgnoreCase(field)) {
                                attributeNumbers[0][i] = j;
                                attributeCount++;
                                break;
                            }
                        }
                    }
                    if (attributeCount != type[0].getColumns().size()) {
                        throw new GPUdbException("Field(s) in set " + set + " missing in file.");
                    }

                    int count = 0;
                    List<CSVRecord> recordList = parser.getRecords();
                    for(CSVRecord record: recordList){
                        if (record.size() != header.size()) {
                            throw new GPUdbException("Error in record " + (count + 1) + ": Incorrect number of fields.");
                        }

                        for (int i = 0; i < record.size(); i++) {
                            int attributeNumber = attributeNumbers[0][i];
                            if (attributeNumber > -1) {
                                String value = record.get(i);
                                Column attribute = type[0].getColumns().get(attributeNumber);
                                try {
                                    if (attribute.getType() == Double.class) {
                                        Double.parseDouble(value);
                                    } else if (attribute.getType() == Float.class) {
                                        Float.parseFloat(value);
                                    } else if (attribute.getType() == Integer.class) {
                                        Integer.parseInt(value);
                                    } else if (attribute.getType() == Long.class) {
                                        Long.parseLong(value);
                                    }
                                } catch (Exception ex) {
                                    throw new GPUdbException("Error in record " + (count + 1) + ": Invalid value \"" + value + "\" for field " + attribute.getName() + ".");
                                }
                            }
                        }
                        count++;
                    }
                } catch (Exception ex) {
                    getLogger().error("Failed to write to set {} at {} in first read", new Object[] { set, gpudb.getURL() }, ex);
                    failed[0] = true;
                }
            }
        });

        if (failed[0]) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                try {
                    CSVParser parser = new CSVParser(new InputStreamReader(in), CSVFormat.RFC4180.withDelimiter(delimiter));
                    parser.iterator().next();
                    List<Record> objects = new ArrayList<>();
                    int count = 0;
                    Type tempType = objectType;
                    for (CSVRecord record : parser) {  
                        Record object = tempType.newInstance();

                        for (int i = 0; i < record.size(); i++) {
                            int attributeNumber = attributeNumbers[0][i];

                            if (attributeNumber > -1) {
                                String value = record.get(i);
                                Column attribute = type[0].getColumns().get(attributeNumber);
                                if (attribute.getType() == Double.class) {                               
                                    object.put(attribute.getName(), Double.parseDouble(value));
                                } else if (attribute.getType() == Float.class) {
                                    object.put(attribute.getName(), Float.parseFloat(value));
                                } else if (attribute.getType() == Integer.class) {
                                    object.put(attribute.getName(), Integer.parseInt(value));
                                } else if (attribute.getType() == java.lang.Long.class) {
                                    object.put(attribute.getName(), Long.parseLong(value));
                                } else {
                                    object.put(attribute.getName(), value);
                                }
                            }
                        }
                        objects.add(object);
                        count++;
                        if (objects.size() == 1000) {
                            gpudb.insertRecords(set, objects, null);
                            objects.clear();
                        }
                    }

                    if (objects.size() > 0) {
                        gpudb.insertRecords(set, objects, null);
                    }

                    getLogger().info("Wrote {} record(s) to set {} at {}.", new Object[] { count, set, gpudb.getURL() });
                } catch (Exception ex) {
                    getLogger().error("Failed to write to set {} at {} in second read", new Object[] { set, gpudb.getURL() }, ex);
                    failed[0] = true;
                }
            }
        });

        if (failed[0]) {
            session.transfer(flowFile, REL_FAILURE);
        } else {
            session.getProvenanceReporter().send(flowFile, gpudb.getURL().toString(), set);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }
}