package com.kinetica.nifi.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.kinetica.nifi.processors.base.AbstractGetKineticaProcessor;

/**
 * NiFi processor that monitors a Kinetica table and outputs new records as Avro.
 *
 * <p>This processor uses Kinetica's table monitor feature (ZeroMQ) to receive
 * real-time notifications of new records inserted into a table, then outputs
 * them as Avro-formatted FlowFiles.
 *
 * <p>Key features:
 * <ul>
 *   <li>Real-time streaming from Kinetica table monitor</li>
 *   <li>Avro binary format output</li>
 *   <li>Automatic schema derivation from table</li>
 *   <li>Batching for efficiency</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "get", "monitor", "stream", "avro"})
@CapabilityDescription("Monitors a Kinetica table for new records using the table monitor feature " +
        "and outputs them as Avro-formatted FlowFiles. This processor maintains a persistent " +
        "connection to receive real-time updates. Requires table monitor URL to be configured in Kinetica.")
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "Number of records in the output"),
        @WritesAttribute(attribute = "mime.type", description = "Set to application/avro-binary"),
        @WritesAttribute(attribute = "avro.schema", description = "The Avro schema as JSON")
})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class GetKineticaToAvro extends AbstractGetKineticaProcessor {

    private static final String PROCESSOR_NAME = "GetKineticaToAvro";

    // ========== AVRO-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .displayName("Batch Size")
            .description("Number of records to batch into each FlowFile. " +
                    "Higher values reduce FlowFile overhead but increase latency.")
            .required(true)
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_INCLUDE_SCHEMA = new PropertyDescriptor.Builder()
            .name("Include Schema")
            .displayName("Include Schema in Output")
            .description("If true, includes the Avro schema in the output (Avro container format). " +
                    "If false, outputs raw Avro binary (schema must be known by consumer).")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    // ========== STATE ==========

    private int batchSize;
    private boolean includeSchema;
    private Schema avroSchema;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(PROP_BATCH_SIZE);
        props.add(PROP_INCLUDE_SCHEMA);
        return props;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();
        includeSchema = context.getProperty(PROP_INCLUDE_SCHEMA).asBoolean();

        // Create Avro schema from Kinetica table type (use parent's objectType)
        Type tableType = getObjectType();
        if (tableType != null) {
            avroSchema = createAvroSchema(tableType);
            getLogger().info("{}: Created Avro schema for table '{}'", PROCESSOR_NAME, tableName);
        }
    }

    @Override
    @OnStopped
    public void onStopped() {
        avroSchema = null;
        super.onStopped();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Drain records from parent's queue
        List<GenericRecord> allRecords = drainRecordQueue();

        // Check if we have enough records for a batch
        if (allRecords.size() < batchSize) {
            // Re-add records to the queue and yield
            if (recordQueue != null && !allRecords.isEmpty()) {
                recordQueue.addAll(allRecords);
            }
            context.yield();
            return;
        }

        // Take only batchSize records
        List<GenericRecord> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize && i < allRecords.size(); i++) {
            batch.add(allRecords.get(i));
        }

        // Re-add remaining records to queue
        if (recordQueue != null && allRecords.size() > batchSize) {
            for (int i = batchSize; i < allRecords.size(); i++) {
                recordQueue.add(allRecords.get(i));
            }
        }

        if (batch.isEmpty()) {
            context.yield();
            return;
        }

        final long startTime = System.currentTimeMillis();

        try {
            // Create FlowFile with Avro data
            FlowFile flowFile = session.create();

            final List<GenericRecord> finalBatch = batch;
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    writeAvroRecords(out, finalBatch);
                }
            });

            // Set attributes
            flowFile = session.putAttribute(flowFile, "record.count", String.valueOf(batch.size()));
            flowFile = session.putAttribute(flowFile, "mime.type", "application/avro-binary");
            flowFile = session.putAttribute(flowFile, "avro.schema", avroSchema.toString());

            final long duration = System.currentTimeMillis() - startTime;

            session.getProvenanceReporter().receive(flowFile,
                    gpudb.getURL().toString() + "/" + tableName,
                    "Received " + batch.size() + " records from table monitor",
                    duration);

            session.transfer(flowFile, REL_SUCCESS);

            getLogger().debug("{}: Output {} records as Avro in {}ms",
                    PROCESSOR_NAME, batch.size(), duration);

        } catch (Exception e) {
            getLogger().error("{}: Error creating Avro output: {}", PROCESSOR_NAME, e.getMessage(), e);
            // Re-queue records for retry
            if (recordQueue != null) {
                recordQueue.addAll(batch);
            }
        }
    }

    /**
     * Creates an Avro schema from a Kinetica Type.
     */
    private Schema createAvroSchema(Type type) {
        List<Schema.Field> fields = new ArrayList<>();

        for (Column column : type.getColumns()) {
            Schema fieldSchema = mapKineticaTypeToAvro(column);

            // Make nullable if column is nullable
            if (column.isNullable()) {
                fieldSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema);
            }

            Schema.Field field = new Schema.Field(
                    column.getName(),
                    fieldSchema,
                    null,  // doc
                    null   // default value
            );
            fields.add(field);
        }

        return Schema.createRecord(
                tableName.replace(".", "_"),  // name (dots not allowed)
                "Auto-generated from Kinetica table " + tableName,  // doc
                "com.kinetica.avro",  // namespace
                false,  // isError
                fields
        );
    }

    /**
     * Maps a Kinetica column type to an Avro schema.
     */
    private Schema mapKineticaTypeToAvro(Column column) {
        Class<?> type = column.getType();

        if (type == String.class) {
            return Schema.create(Schema.Type.STRING);
        } else if (type == Integer.class) {
            return Schema.create(Schema.Type.INT);
        } else if (type == Long.class) {
            return Schema.create(Schema.Type.LONG);
        } else if (type == Float.class) {
            return Schema.create(Schema.Type.FLOAT);
        } else if (type == Double.class) {
            return Schema.create(Schema.Type.DOUBLE);
        } else if (type == ByteBuffer.class || type == byte[].class) {
            return Schema.create(Schema.Type.BYTES);
        } else {
            // Default to string for unknown types
            return Schema.create(Schema.Type.STRING);
        }
    }

    /**
     * Writes records as Avro binary format.
     */
    private void writeAvroRecords(OutputStream out, List<GenericRecord> records) throws IOException {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);

        if (includeSchema) {
            // Use Avro container format with schema
            org.apache.avro.file.DataFileWriter<GenericRecord> dataFileWriter =
                    new org.apache.avro.file.DataFileWriter<>(datumWriter);
            dataFileWriter.create(avroSchema, out);

            for (GenericRecord record : records) {
                dataFileWriter.append(record);
            }

            dataFileWriter.close();
        } else {
            // Raw Avro binary without schema
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

            for (GenericRecord record : records) {
                datumWriter.write(record, encoder);
            }

            encoder.flush();
        }
    }
}
