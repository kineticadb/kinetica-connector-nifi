package com.gisfederal.gpudb.processors.GPUdbNiFi;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase.Options;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type;
import com.gpudb.protocol.ExecuteSqlResponse;
import com.gpudb.protocol.GetRecordsResponse;

@Tags({"Kinetica", "query", "get", "json"})
@CapabilityDescription("Queries a Kinetica table or executes a SQL query and outputs the results as JSON array. "
        + "Supports batching with configurable batch size.")
@WritesAttributes({
    @WritesAttribute(attribute = "mime.type", description = "Sets MIME type to application/json"),
    @WritesAttribute(attribute = "kinetica.record.count", description = "Number of records in this FlowFile"),
    @WritesAttribute(attribute = "kinetica.batch.number", description = "Batch sequence number"),
    @WritesAttribute(attribute = "kinetica.total.records", description = "Total records available")
})
public class QueryKineticaToJSON extends AbstractProcessor {

    public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder()
            .name(KineticaConstants.SERVER_URL)
            .description("URL of the Kinetica server")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_TABLE = new PropertyDescriptor.Builder()
            .name(KineticaConstants.TABLE_NAME)
            .description("Full table name (e.g. demo.mytable)")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_SQL_QUERY = new PropertyDescriptor.Builder()
            .name(KineticaConstants.SQL_QUERY)
            .description("Custom SQL query (e.g. SELECT * FROM demo.mytable WHERE id > 100)")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name(KineticaConstants.BATCH_SIZE)
            .description("Number of records per FlowFile batch")
            .required(true)
            .defaultValue("10000")
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
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
            .description("FlowFiles with query results in JSON format")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name(KineticaConstants.FAILURE)
            .description("FlowFiles routed here on error")
            .build();

    private GPUdb gpudb;
    private String tableName;
    private String sqlQuery;
    private boolean useTableMode;
    private int batchSize;
    private Type objectType;
    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptorsList = new ArrayList<>();
        descriptorsList.add(PROP_SERVER);
        descriptorsList.add(PROP_TABLE);
        descriptorsList.add(PROP_SQL_QUERY);
        descriptorsList.add(PROP_BATCH_SIZE);
        descriptorsList.add(PROP_USERNAME);
        descriptorsList.add(PROP_PASSWORD);
        this.descriptors = Collections.unmodifiableList(descriptorsList);

        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(REL_SUCCESS);
        relationshipSet.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationshipSet);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>();
        boolean hasTable = context.getProperty(PROP_TABLE).isSet();
        boolean hasSql = context.getProperty(PROP_SQL_QUERY).isSet();
        if (hasTable == hasSql) {
            results.add(new ValidationResult.Builder()
                    .subject("Table Name / SQL Query")
                    .valid(false)
                    .explanation("Exactly one of 'Table Name' or 'SQL Query' must be specified")
                    .build());
        }
        return results;
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

        batchSize = context.getProperty(PROP_BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        if (context.getProperty(PROP_TABLE).isSet()) {
            useTableMode = true;
            tableName = context.getProperty(PROP_TABLE).evaluateAttributeExpressions().getValue();
            objectType = Type.fromTable(gpudb, tableName);
        } else {
            useTableMode = false;
            sqlQuery = context.getProperty(PROP_SQL_QUERY).evaluateAttributeExpressions().getValue();
            objectType = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        long offset = 0;
        int batchNumber = 0;
        Type type = this.objectType;

        try {
            boolean hasMore = true;
            while (hasMore) {
                final List<Record> records;
                final long totalRecords;

                if (useTableMode) {
                    GetRecordsResponse<Record> response = gpudb.getRecords(tableName, offset, batchSize, null);
                    records = response.getData();
                    totalRecords = response.getTotalNumberOfRecords();
                    hasMore = response.getHasMoreRecords();
                } else {
                    ExecuteSqlResponse response = gpudb.executeSql(sqlQuery, offset, batchSize, null, null, null);
                    records = response.getData();
                    totalRecords = response.getTotalNumberOfRecords();
                    hasMore = response.getHasMoreRecords();
                }

                if (records == null || records.isEmpty()) {
                    break;
                }

                if (type == null) {
                    type = records.get(0).getType();
                }

                batchNumber++;
                final int currentBatch = batchNumber;
                final long currentTotal = totalRecords;
                final Type currentType = type;

                FlowFile flowFile = session.create();
                flowFile = session.write(flowFile, (OutputStream out) -> {
                    writeJSON(out, records, currentType);
                });

                final Map<String, String> attributes = new HashMap<>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                attributes.put(CoreAttributes.FILENAME.key(),
                        flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
                attributes.put("kinetica.table", useTableMode ? tableName : "sql_query");
                attributes.put("kinetica.record.count", String.valueOf(records.size()));
                attributes.put("kinetica.batch.number", String.valueOf(currentBatch));
                attributes.put("kinetica.total.records", String.valueOf(currentTotal));
                flowFile = session.putAllAttributes(flowFile, attributes);

                session.getProvenanceReporter().receive(flowFile, gpudb.getURL().toString());
                session.transfer(flowFile, REL_SUCCESS);

                getLogger().info("Batch {} - wrote {} JSON records from {} at {}",
                        new Object[]{currentBatch, records.size(),
                                useTableMode ? tableName : "SQL query", gpudb.getURL()});

                offset += records.size();
            }

            if (batchNumber == 0) {
                getLogger().debug("No records returned from query");
                context.yield();
            }
        } catch (GPUdbException e) {
            getLogger().error("Error querying Kinetica: {}", new Object[]{e.getMessage()}, e);
            FlowFile errorFlowFile = session.create();
            errorFlowFile = session.putAttribute(errorFlowFile, "error.message", e.getMessage());
            session.transfer(errorFlowFile, REL_FAILURE);
        } catch (ProcessException e) {
            throw e;
        } catch (Exception e) {
            getLogger().error("Unexpected error querying Kinetica: {}", new Object[]{e.getMessage()}, e);
            FlowFile errorFlowFile = session.create();
            errorFlowFile = session.putAttribute(errorFlowFile, "error.message", e.getMessage());
            session.transfer(errorFlowFile, REL_FAILURE);
        }
    }

    private void writeJSON(OutputStream out, List<Record> records, Type type) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        ArrayNode arrayNode = mapper.createArrayNode();

        List<Type.Column> columns = type.getColumns();

        for (Record record : records) {
            ObjectNode objectNode = mapper.createObjectNode();
            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i).getName();
                Object value = record.get(i);
                if (value == null) {
                    objectNode.putNull(name);
                } else if (value instanceof Integer) {
                    objectNode.put(name, (Integer) value);
                } else if (value instanceof Long) {
                    objectNode.put(name, (Long) value);
                } else if (value instanceof Float) {
                    objectNode.put(name, (Float) value);
                } else if (value instanceof Double) {
                    objectNode.put(name, (Double) value);
                } else if (value instanceof ByteBuffer) {
                    byte[] bytes = new byte[((ByteBuffer) value).remaining()];
                    ((ByteBuffer) value).duplicate().get(bytes);
                    objectNode.put(name, Base64.getEncoder().encodeToString(bytes));
                } else {
                    objectNode.put(name, value.toString());
                }
            }
            arrayNode.add(objectNode);
        }

        mapper.writeValue(out, arrayNode);
    }
}
