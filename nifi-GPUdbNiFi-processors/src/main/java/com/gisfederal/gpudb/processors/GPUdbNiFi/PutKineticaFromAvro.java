package com.gisfederal.gpudb.processors.GPUdbNiFi;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.BulkInserter;
import com.gpudb.ColumnProperty;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase.Options;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.WorkerList;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableResponse;
import com.gpudb.protocol.InsertRecordsRequest;

@Tags({"Kinetica", "add", "bulkadd", "put", "avro"})
@CapabilityDescription("Reads Avro container format data from a FlowFile and inserts records into a Kinetica table via "
        + "the native GPUdb BulkInserter API. The FlowFile content must be a valid Avro container file (with embedded schema). "
        + "Each Avro record's fields must correspond to the column names of the target Kinetica table. "
        + "If the table does not exist, it will be created from the provided Schema property. "
        + "Example schema: x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search")
@ReadsAttribute(attribute = "mime.type", description = "Determines MIME type of input content")
public class PutKineticaFromAvro extends AbstractProcessor {

    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name(KineticaConstants.SERVER_URL)
            .description("URL of the Kinetica server. Example http://172.3.4.19:9191")
            .required(true)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_TABLE = new PropertyDescriptor.Builder()
            .name(KineticaConstants.TABLE_NAME)
            .description("Name of the Kinetica table. Use schema-qualified names (e.g. 'myschema.mytable') for schema support.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_SCHEMA = new PropertyDescriptor.Builder()
            .name(KineticaConstants.SCHEMA)
            .description("Schema of the Kinetica table. Schema not required if table exists in Kinetica already."
                    + " Example schema: x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search,AUTHOR|String|text_search|data")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_AVRO_SCHEMA = new PropertyDescriptor.Builder()
            .name(KineticaConstants.AVRO_SCHEMA)
            .description("Avro schema JSON defining the Kinetica table structure. "
                    + "This is the standard NiFi Avro schema format. "
                    + "Used only if the table does not exist and the pipe-delimited Schema property is not set. "
                    + "Avro types are mapped to Kinetica types: string→String, int→Integer, long→Long, float→Float, double→Double, boolean→Integer(0/1), bytes→Bytes.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name(KineticaConstants.BATCH_SIZE)
            .description("Batch size of bulk load to Kinetica.")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("500")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder()
            .name(KineticaConstants.USERNAME)
            .description("Username to connect to Kinetica")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder()
            .name(KineticaConstants.PASSWORD)
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

    protected static final PropertyDescriptor UPDATE_ON_EXISTING_PK = new PropertyDescriptor.Builder()
            .name(KineticaConstants.UPDATE_ON_EXISTING_PK)
            .description("If the table has a primary key, then if the value is 'true' and any of the records being added "
                    + "have the same primary key as existing records, the existing records are replaced (i.e. *updated*). "
                    + "If 'false', records with existing primary keys are ignored.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    protected static final PropertyDescriptor PROP_REPLICATE_TABLE = new PropertyDescriptor.Builder()
            .name(KineticaConstants.REPLICATE_TABLE)
            .description("If the Kinetica table doesn't already exist then it will be created by this processor. "
                    + "A value of true indicates that the table that is created should be replicated.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor PROP_DATE_FORMAT = new PropertyDescriptor.Builder()
            .name(KineticaConstants.DATE_FORMAT)
            .description("Provide the date format used for your datetime values. Example: yyyy/MM/dd HH:mm:ss")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor PROP_TIMEZONE = new PropertyDescriptor.Builder()
            .name(KineticaConstants.TIMEZONE)
            .description("Provide the timezone the data was created in. If no timezone is set, the current timezone will be used. Example: EST")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name(KineticaConstants.SUCCESS)
            .description("All FlowFiles that are successfully written to Kinetica are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name(KineticaConstants.FAILURE)
            .description("All FlowFiles that cannot be written to Kinetica are routed to this relationship")
            .build();

    private GPUdb gpudb;
    private String tableName;
    public Type objectType;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    private boolean updateOnExistingPk;
    private String dateFormat;
    private String timeZone;
    private BulkInserter<Record> bulkInserter;
    private WorkerList workers;
    private int batchSize;
    private static final String PROCESSOR_NAME = "PutKineticaFromAvro";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_SERVER);
        descriptors.add(PROP_TABLE);
        descriptors.add(PROP_SCHEMA);
        descriptors.add(PROP_AVRO_SCHEMA);
        descriptors.add(PROP_BATCH_SIZE);
        descriptors.add(PROP_USERNAME);
        descriptors.add(PROP_PASSWORD);
        descriptors.add(PROP_DISABLE_AUTO_DISCOVERY);
        descriptors.add(PROP_DISABLE_FAILOVER);
        descriptors.add(UPDATE_ON_EXISTING_PK);
        descriptors.add(PROP_REPLICATE_TABLE);
        descriptors.add(PROP_DATE_FORMAT);
        descriptors.add(PROP_TIMEZONE);
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

    private Type createTable(ProcessContext context, String schemaStr) throws GPUdbException {
        getLogger().info(PROCESSOR_NAME + " created table in Kinetica:" + tableName + ", schemaStr:" + schemaStr);
        HasTableResponse response = gpudb.hasTable(tableName, null);
        if (response.getTableExists()) {
            return null;
        }
        List<Column> attributes = new ArrayList<>();
        int maxPrimaryKey = -1;
        String[] fieldArray = schemaStr.split(",");
        for (String fieldStr : fieldArray) {
            String[] split = fieldStr.split("\\|", -1);
            String name = split[0];
            Class<?> type;
            getLogger().info(PROCESSOR_NAME + ": Field name '" + name + "', type '" + split[1].toLowerCase() + "'");
            if (split.length > 1) {
                switch (split[1].toLowerCase()) {
                    case "double":
                        type = Double.class;
                        break;
                    case "float":
                        type = Float.class;
                        break;
                    case "integer":
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
                        maxPrimaryKey = Math.max(keyIndex, maxPrimaryKey);
                    } else {
                        ++maxPrimaryKey;
                    }
                } else {
                    annotations.add(annotation);
                }
            }
            attributes.add(new Column(name, type, annotations));
        }
        getLogger().info(PROCESSOR_NAME + ": creating Kinetica type " + attributes);
        Type type = new Type("", attributes);

        String typeId = type.create(gpudb);
        response = gpudb.hasTable(tableName, null);

        if (!response.getTableExists()) {
            boolean replicated_flag = context.getProperty(PROP_REPLICATE_TABLE).isSet()
                    && context.getProperty(PROP_REPLICATE_TABLE).asBoolean().booleanValue();
            getLogger().debug(PROCESSOR_NAME + " replicated_flag = " + replicated_flag);

            Map<String, String> create_table_options = GPUdb.options(
                    CreateTableRequest.Options.IS_REPLICATED,
                    replicated_flag ? CreateTableRequest.Options.TRUE : CreateTableRequest.Options.FALSE);

            getLogger().debug(PROCESSOR_NAME + " create_table_options has " + create_table_options.size() + " properties");
            gpudb.createTable(tableName, typeId, create_table_options);
        }

        gpudb.addKnownType(typeId, RecordObject.class);
        return type;
    }

    private Type createTableFromAvroSchema(ProcessContext context, String avroSchemaJson) throws GPUdbException {
        getLogger().info(PROCESSOR_NAME + " creating table from Avro schema: " + tableName);
        HasTableResponse response = gpudb.hasTable(tableName, null);
        if (response.getTableExists()) {
            return null;
        }

        Schema avroSchema;
        try {
            avroSchema = new Schema.Parser().parse(avroSchemaJson);
        } catch (Exception e) {
            throw new GPUdbException("Failed to parse Avro schema JSON: " + e.getMessage());
        }

        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new GPUdbException("Avro schema must be a record type, got: " + avroSchema.getType());
        }

        List<Column> columns = new ArrayList<>();
        for (Schema.Field field : avroSchema.getFields()) {
            Schema fieldSchema = field.schema();
            boolean isNullable = false;

            // Unwrap union types (e.g., ["null", "string"] → "string") and detect nullability
            if (fieldSchema.getType() == Schema.Type.UNION) {
                for (Schema unionType : fieldSchema.getTypes()) {
                    if (unionType.getType() == Schema.Type.NULL) {
                        isNullable = true;
                    }
                }
                for (Schema unionType : fieldSchema.getTypes()) {
                    if (unionType.getType() != Schema.Type.NULL) {
                        fieldSchema = unionType;
                        break;
                    }
                }
            }

            // Detect Avro logical types (timestamp-millis, date, time-millis, decimal)
            String logicalType = null;
            if (fieldSchema.getLogicalType() != null) {
                logicalType = fieldSchema.getLogicalType().getName();
            } else if (fieldSchema.getObjectProp("logicalType") != null) {
                logicalType = fieldSchema.getObjectProp("logicalType").toString();
            }

            // Map Avro type + logical type → Kinetica type + column properties
            Class<?> kineticaType;
            List<String> properties = new ArrayList<>();
            properties.add(ColumnProperty.DATA);

            if (logicalType != null) {
                switch (logicalType) {
                    case "timestamp-millis":
                    case "timestamp-micros":
                        kineticaType = Long.class;
                        properties.add(ColumnProperty.TIMESTAMP);
                        break;
                    case "date":
                        kineticaType = String.class;
                        properties.add(ColumnProperty.DATE);
                        break;
                    case "time-millis":
                    case "time-micros":
                        kineticaType = String.class;
                        properties.add(ColumnProperty.TIME);
                        break;
                    case "decimal":
                        kineticaType = String.class;
                        properties.add(ColumnProperty.DECIMAL);
                        break;
                    default:
                        // Unknown logical type — fall through to base type mapping
                        kineticaType = mapAvroBaseType(fieldSchema, field.name());
                        break;
                }
            } else {
                kineticaType = mapAvroBaseType(fieldSchema, field.name());
                // Boolean maps to int8 in Kinetica
                if (fieldSchema.getType() == Schema.Type.BOOLEAN) {
                    properties.add(ColumnProperty.INT8);
                }
            }

            if (isNullable) {
                properties.add(ColumnProperty.NULLABLE);
            }

            columns.add(new Column(field.name(), kineticaType, properties));
            getLogger().debug(PROCESSOR_NAME + " Avro field '" + field.name() + "' → Kinetica "
                    + kineticaType.getSimpleName() + " " + properties);
        }

        Type objectType = new Type(columns);
        String typeId = objectType.create(gpudb);

        boolean replicateTable = false;
        try {
            if (context.getProperty(PROP_REPLICATE_TABLE) != null && context.getProperty(PROP_REPLICATE_TABLE).isSet()) {
                replicateTable = context.getProperty(PROP_REPLICATE_TABLE).asBoolean();
            }
        } catch (Exception e) {
            // Property may not exist in all processors
        }

        Map<String, String> createOptions = new LinkedHashMap<>();
        if (replicateTable) {
            createOptions.put(CreateTableRequest.Options.IS_REPLICATED, CreateTableRequest.Options.TRUE);
        }

        gpudb.createTable(tableName, typeId, createOptions);
        getLogger().info(PROCESSOR_NAME + " created table " + tableName + " from Avro schema with " + columns.size() + " columns");

        return objectType;
    }

    private Class<?> mapAvroBaseType(Schema fieldSchema, String fieldName) {
        switch (fieldSchema.getType()) {
            case STRING:
            case ENUM:
                return String.class;
            case INT:
                return Integer.class;
            case LONG:
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case BOOLEAN:
                return Integer.class;
            case BYTES:
            case FIXED:
                return ByteBuffer.class;
            default:
                getLogger().warn(PROCESSOR_NAME + " Unsupported Avro type '" + fieldSchema.getType()
                        + "' for field '" + fieldName + "', defaulting to String");
                return String.class;
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws GPUdbException {
        Options option = new Options();
        if (context.getProperty(PROP_USERNAME).evaluateAttributeExpressions().getValue() != null
                && context.getProperty(PROP_PASSWORD).getValue() != null) {
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

        tableName = context.getProperty(PROP_TABLE).evaluateAttributeExpressions().getValue();
        updateOnExistingPk = context.getProperty(UPDATE_ON_EXISTING_PK).asBoolean().booleanValue();
        dateFormat = context.getProperty(PROP_DATE_FORMAT).evaluateAttributeExpressions().getValue();
        timeZone = context.getProperty(PROP_TIMEZONE).evaluateAttributeExpressions().getValue();

        if (KineticaUtilities.tableExists(gpudb, tableName, getLogger())) {
            getLogger().debug(PROCESSOR_NAME + " Getting type from table:" + tableName);
            objectType = Type.fromTable(gpudb, tableName);
            getLogger().debug(PROCESSOR_NAME + " objectType:" + objectType.toString());
        } else if (context.getProperty(PROP_SCHEMA).isSet()) {
            objectType = createTable(context, context.getProperty(PROP_SCHEMA).evaluateAttributeExpressions().getValue());
        } else if (context.getProperty(PROP_AVRO_SCHEMA).isSet()) {
            objectType = createTableFromAvroSchema(context, context.getProperty(PROP_AVRO_SCHEMA).evaluateAttributeExpressions().getValue());
        } else {
            objectType = null;
        }

        batchSize = context.getProperty(PROP_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        workers = new WorkerList(gpudb);

        // Pre-create BulkInserter if type is already known
        if (objectType != null) {
            bulkInserter = new BulkInserter<>(gpudb, tableName, objectType, batchSize,
                GPUdb.options(InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK,
                    updateOnExistingPk ? InsertRecordsRequest.Options.TRUE : InsertRecordsRequest.Options.FALSE),
                workers);
        }
    }

    private synchronized BulkInserter<Record> ensureBulkInserter() throws GPUdbException {
        if (bulkInserter == null && objectType != null) {
            bulkInserter = new BulkInserter<>(gpudb, tableName, objectType, batchSize,
                GPUdb.options(InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK,
                    updateOnExistingPk ? InsertRecordsRequest.Options.TRUE : InsertRecordsRequest.Options.FALSE),
                workers);
        }
        return bulkInserter;
    }

    @OnStopped
    public void onStopped() {
        if (bulkInserter != null) {
            try {
                bulkInserter.flush();
            } catch (Exception e) {
                getLogger().warn("Error flushing BulkInserter on stop: " + e.getMessage(), e);
            }
            bulkInserter = null;
        }
        workers = null;
        gpudb = null;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String effectiveTableName = context.getProperty(PROP_TABLE)
                .evaluateAttributeExpressions().getValue();
        final String effectiveDateFormat = context.getProperty(PROP_DATE_FORMAT).isSet()
                ? context.getProperty(PROP_DATE_FORMAT).evaluateAttributeExpressions().getValue()
                : dateFormat;
        final String effectiveTimeZone = context.getProperty(PROP_TIMEZONE).isSet()
                ? context.getProperty(PROP_TIMEZONE).evaluateAttributeExpressions().getValue()
                : timeZone;

        try {
            if (!KineticaUtilities.tableExists(gpudb, effectiveTableName, getLogger())) {
                throw new ProcessException(PROCESSOR_NAME + " Error: Table '" + effectiveTableName
                        + "' does not exist in Kinetica. Please provide a schema or create the table prior to loading data.");
            }
            ensureBulkInserter();
        } catch (Exception e) {
            throw new ProcessException(PROCESSOR_NAME + " Error: Failed to create BulkInserter " + e.getMessage()
                    + "; for debugging purposes, here is the stack trace:\n"
                    + KineticaUtilities.convertStacktraceToString(e));
        }

        boolean failed = false;
        int count = 0;
        int errorCount = 0;

        try (InputStream istream = session.read(flowFile)) {
            Type tempType = objectType;
            if (tempType == null) {
                if (context.getProperty(PROP_SCHEMA).isSet()) {
                    tempType = createTable(context, context.getProperty(PROP_SCHEMA)
                            .evaluateAttributeExpressions().getValue());
                    objectType = tempType;
                } else if (context.getProperty(PROP_AVRO_SCHEMA).isSet()) {
                    tempType = createTableFromAvroSchema(context, context.getProperty(PROP_AVRO_SCHEMA)
                            .evaluateAttributeExpressions().getValue());
                    objectType = tempType;
                } else {
                    throw new ProcessException(PROCESSOR_NAME + " Error: No table and no schema defined.");
                }
            }

            int numColumns = tempType.getColumnCount();

            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
            try (DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(istream, datumReader)) {
                while (dataFileStream.hasNext()) {
                    GenericRecord avroRecord = dataFileStream.next();
                    Record object = tempType.newInstance();
                    boolean isRecordBad = false;

                    for (int i = 0; i < numColumns; i++) {
                        Column column = tempType.getColumn(i);
                        String colName = column.getName();
                        Object avroValue = avroRecord.get(colName);

                        try {
                            if (avroValue == null) {
                                if (column.isNullable()) {
                                    object.put(colName, null);
                                } else {
                                    throw new GPUdbException("Found null value for non-nullable column " + colName);
                                }
                            } else {
                                boolean timeStamp = KineticaUtilities.checkForTimeStamp(column);
                                String valueStr = avroValue.toString();

                                if (timeStamp) {
                                    if (StringUtils.isNumeric(valueStr)) {
                                        long valueLong;
                                        try {
                                            valueLong = Long.parseLong(valueStr);
                                        } catch (NumberFormatException ex) {
                                            valueLong = 0;
                                        }
                                        object.put(colName, valueLong);
                                    } else {
                                        Long timestamp = KineticaUtilities.parseDate(valueStr, effectiveDateFormat,
                                                effectiveTimeZone, getLogger());
                                        if (timestamp != null) {
                                            object.put(colName, timestamp);
                                        } else {
                                            getLogger().error(PROCESSOR_NAME + " Error: Failed to parse date. "
                                                    + "Please check your date format and try again.");
                                            isRecordBad = true;
                                            throw new GPUdbException("Bad timestamp given: '" + valueStr + "'");
                                        }
                                    }
                                } else if (column.getType() == Double.class) {
                                    if (avroValue instanceof Number) {
                                        object.put(colName, ((Number) avroValue).doubleValue());
                                    } else {
                                        object.put(colName, Double.parseDouble(valueStr));
                                    }
                                } else if (column.getType() == Float.class) {
                                    if (avroValue instanceof Number) {
                                        object.put(colName, ((Number) avroValue).floatValue());
                                    } else {
                                        object.put(colName, Float.parseFloat(valueStr));
                                    }
                                } else if (column.getType() == Integer.class) {
                                    if (avroValue instanceof Number) {
                                        object.put(colName, ((Number) avroValue).intValue());
                                    } else {
                                        object.put(colName, Integer.parseInt(valueStr));
                                    }
                                } else if (column.getType() == Long.class) {
                                    if (avroValue instanceof Number) {
                                        object.put(colName, ((Number) avroValue).longValue());
                                    } else {
                                        object.put(colName, Long.parseLong(valueStr));
                                    }
                                } else {
                                    String strVal = valueStr.trim();
                                    if (!strVal.isEmpty()) {
                                        object.put(colName, strVal);
                                    }
                                }
                            }
                        } catch (GPUdbException e) {
                            errorCount++;
                            getLogger().warn(PROCESSOR_NAME + " Warning: Skipping record " + (count + 1)
                                    + ": Invalid value for field " + colName + ". Total error count = " + errorCount);
                            isRecordBad = true;
                            break;
                        } catch (Exception e) {
                            errorCount++;
                            getLogger().warn(PROCESSOR_NAME + " Warning: Skipping record " + (count + 1)
                                    + ": Error processing field " + colName + ": " + e.getMessage());
                            isRecordBad = true;
                            break;
                        }
                    }

                    if (!isRecordBad) {
                        try {
                            bulkInserter.insert(object);
                        } catch (BulkInserter.InsertException e) {
                            getLogger().error(PROCESSOR_NAME + " Error: " + e.getMessage());
                        }
                    }
                    count++;
                }
            }

            try {
                bulkInserter.flush();
            } catch (BulkInserter.InsertException e) {
                getLogger().error(PROCESSOR_NAME + " Error: " + e.getMessage());
            }

            getLogger().info(PROCESSOR_NAME + ": Wrote {} record(s) to set {} at {}.",
                    new Object[]{count - errorCount, effectiveTableName, gpudb.getURL()});
        } catch (ProcessException pe) {
            throw pe;
        } catch (Exception ex) {
            getLogger().error(PROCESSOR_NAME + " Error: Failed to write to set {} at {}",
                    new Object[]{effectiveTableName, gpudb.getURL()}, ex);
            failed = true;
        }

        if (failed) {
            flowFile = session.putAttribute(flowFile, "error.message",
                    PROCESSOR_NAME + ": Failed to process Avro content");
            session.transfer(flowFile, REL_FAILURE);
        } else {
            session.getProvenanceReporter().send(flowFile, gpudb.getURL().toString(), effectiveTableName);
            session.transfer(flowFile, REL_SUCCESS);
        }
    }
}
