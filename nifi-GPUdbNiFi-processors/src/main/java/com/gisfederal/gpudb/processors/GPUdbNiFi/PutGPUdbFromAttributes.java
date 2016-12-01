package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableResponse;
import com.gpudb.protocol.InsertRecordsRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
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
public class PutGPUdbFromAttributes extends AbstractProcessor {
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


    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptorList = new ArrayList<>();
        descriptorList.add(PROP_SERVER);
        descriptorList.add(PROP_PARENT_SET);
        descriptorList.add(PROP_SET);
        descriptorList.add(PROP_SCHEMA);
        descriptorList.add(PROP_LABEL);
        descriptorList.add(PROP_SEMANTIC_TYPE);
        descriptorList.add(PROP_VERSION);
        this.descriptors = Collections.unmodifiableList(descriptorList);

        final Set<Relationship> relationshipList = new HashSet<>();
        relationshipList.add(REL_SUCCESS);
        relationshipList.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationshipList);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }
    
    private Type createSet(ProcessContext context, String schemaStr)throws GPUdbException {
        getLogger().info("GPUdb-createSet:" + set + ", schemaStr:" + schemaStr);
        HasTableResponse response = gpudb.hasTable(set, null);
        if(response.getTableExists()){
            return(null);
        }
        List<Column> attributes = new ArrayList<>();
        int maxPrimaryKey = -1;     
        String[] fieldArray = schemaStr.split(",");
        for(String fieldStr : fieldArray){
            String[] split = fieldStr.split("\\|", -1);
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

                    case "integer":
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

            int primaryKey;
            List<String> annotations = new ArrayList<>();

            for (int j = 2; j < split.length; j++) {
                String annotation = split[j].toLowerCase().trim();

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
        getLogger().info("GPUdb-type:" + attributes);
        Type type = new Type(context.getProperty(PROP_LABEL).isSet() ? context.getProperty(PROP_LABEL).getValue() : "",attributes);

        
        String typeId = type.create(gpudb);
        response = gpudb.hasTable(set, null);
        Map<String, String> create_table_options;
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
             objectType = createSet(context, context.getProperty(PROP_SCHEMA).getValue());
        } else {
            objectType = null;
        }

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        final Relationship[] relationshipStatus = new Relationship[1]; //This is an array of 1 because in order to declare it here 
                                                                       //and use it in an inner class, it has be be declared final.

        if (flowFile == null) {
            return;
        }

        if (!flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()).equals("application/json")) {
            getLogger().error("Unsupported MIME type \"{}\".", new Object[] { flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()) });
            session.transfer(flowFile, REL_FAILURE);
            return;
        }


        session.read(flowFile, new InputStreamCallback() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
            public void process(InputStream in) throws IOException {
                try {
                    Type tempType = objectType;
                    Record object = tempType.newInstance();
                    String value;

                    
                    Map attributeMap = flowFile.getAttributes();
                    for(Column column : objectType.getColumns()){
                        String columnName   = column.getName();
                        String propertyName = "gpudb." + columnName;
                        if(attributeMap.containsKey(propertyName)){
                            value = attributeMap.get(propertyName).toString();
                        }
                        else{
                            value="";
                        }

                        if (column.getType() == Double.class) {
                            double valueDouble;
                            try{
                                valueDouble = Double.parseDouble(value);
                            }
                            catch(NumberFormatException ex){
                                valueDouble = 0;
                            }
                            object.put(columnName,valueDouble);
                        } else if (column.getType() == Float.class) {
                            float valueFloat;
                            try{
                                valueFloat = Float.parseFloat(value);
                            }
                            catch(NumberFormatException ex){
                                valueFloat = 0;
                            }                                
                            object.put(columnName,valueFloat);
                        } else if (column.getType() == Integer.class) {
                            int valueInt;
                            try{
                                valueInt = Integer.parseInt(value);
                            }
                            catch(NumberFormatException ex){
                                valueInt = 0;
                            } 
                            object.put(columnName, valueInt);
                        } else if (column.getType() == java.lang.Long.class) {
                            long valueLong;
                            try{
                                valueLong = Long.parseLong(value);
                            }
                            catch(NumberFormatException ex){
                                valueLong = 0;
                            } 

                            object.put(columnName, valueLong);
                        } else {
                            object.put(columnName, value);
                        }

                        getLogger().info("GPUdb-Found {} property with value {} inserting into column {}.", new Object[] { propertyName, value, columnName });
                    }   
                    List<Record> objects = new ArrayList<>();                    
                    objects.add(object);
                    
                    getLogger().info("GPUdb - writing record {} to set {} at {}.", new Object[] { objects.get(0), set, gpudb.getURL() });
                    InsertRecordsRequest req = new InsertRecordsRequest();
                    req.setData(objects);
                    req.setTableName(set);
                    HashMap<String,String> options = new HashMap<>();
                    req.setOptions(options);
                    gpudb.insertRecords(req);
                    objects.clear();
                    relationshipStatus[0] = REL_SUCCESS;
                } catch (GPUdbException ex) {
                    getLogger().error("GPUdb - Failed to write to set {} at {} in second read", new Object[] { set, gpudb.getURL() }, ex);
                    relationshipStatus[0] = REL_FAILURE;
                }
               
            }
        });
        session.transfer(flowFile, relationshipStatus[0]);
    }
}