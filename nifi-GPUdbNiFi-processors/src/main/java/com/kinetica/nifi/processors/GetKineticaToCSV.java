package com.kinetica.nifi.processors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.Type;
import com.kinetica.nifi.processors.base.AbstractGetKineticaProcessor;

/**
 * NiFi processor that monitors a Kinetica table and outputs new records as CSV files.
 *
 * <p>This processor uses Kinetica's table monitor feature via ZeroMQ to receive
 * notifications of new records and outputs them as CSV-formatted FlowFiles.
 *
 * <p>Key features:
 * <ul>
 *   <li>Real-time monitoring via ZeroMQ subscription</li>
 *   <li>Configurable CSV delimiter</li>
 *   <li>Header row with column names and types</li>
 *   <li>Proper resource cleanup (ZeroMQ connection leak fixed)</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "get", "csv", "monitor", "stream"})
@CapabilityDescription("Monitors a Kinetica table for new records and outputs them as CSV files. " +
        "Uses ZeroMQ table monitor for real-time notifications. " +
        "The output includes a header row with column names and types in Kinetica schema format.")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to text/csv")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class GetKineticaToCSV extends AbstractGetKineticaProcessor {

    private static final String PROCESSOR_NAME = "GetKineticaToCSV";

    // ========== CSV-SPECIFIC PROPERTY DESCRIPTORS ==========

    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder()
            .name("Delimiter")
            .displayName("Delimiter")
            .description("Field delimiter for CSV output. Default is tab character.")
            .required(false)
            .defaultValue("\t")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private char delimiter;

    @Override
    protected List<PropertyDescriptor> getAdditionalPropertyDescriptors() {
        return Collections.singletonList(PROP_DELIMITER);
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) throws ProcessException {
        super.onScheduled(context);

        String delimStr = context.getProperty(PROP_DELIMITER).getValue();
        delimiter = KineticaUtilities.parseSpecialChar(delimStr, '\t');
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Drain the record queue
        List<GenericRecord> records = drainRecordQueue();

        if (records.isEmpty()) {
            context.yield();
            return;
        }

        // Create output FlowFile
        FlowFile flowFile = session.create();

        final Type type = getObjectType();
        final int recordCount = records.size();

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
                     CSVPrinter printer = new CSVPrinter(writer, CSVFormat.RFC4180.builder()
                             .setDelimiter(delimiter)
                             .build())) {

                    // Write header row with column info
                    List<String> headerFields = buildHeaderFields(type);
                    printer.printRecord(headerFields);

                    // Write data records
                    for (GenericRecord record : records) {
                        List<String> fields = new ArrayList<>();
                        for (int i = 0; i < type.getColumnCount(); i++) {
                            Object value = record.get(i);
                            fields.add(value != null ? value.toString() : "");
                        }
                        printer.printRecord(fields);
                    }

                    printer.flush();
                }
            }
        });

        // Set attributes
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        attributes.put(CoreAttributes.FILENAME.key(),
                flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".csv");
        attributes.put("record.count", String.valueOf(recordCount));
        attributes.put("kinetica.table", tableName);
        flowFile = session.putAllAttributes(flowFile, attributes);

        // Transfer to success
        session.getProvenanceReporter().receive(flowFile, gpudb.getURL().toString(), tableName);
        session.transfer(flowFile, REL_SUCCESS);

        getLogger().info("{}: Output {} records from table '{}' as CSV",
                PROCESSOR_NAME, recordCount, tableName);
    }

    /**
     * Builds header fields with column name and type information.
     * Format: "name|type|annotation1|annotation2"
     */
    private List<String> buildHeaderFields(Type type) {
        List<String> fields = new ArrayList<>();

        for (Type.Column column : type.getColumns()) {
            StringBuilder field = new StringBuilder();
            field.append(column.getName()).append("|");
            field.append(KineticaUtilities.mapTypeToSchemaName(column.getType()));

            // Add column properties/annotations
            for (String property : column.getProperties()) {
                field.append("|").append(property);
            }

            fields.add(field.toString());
        }

        return fields;
    }
}
