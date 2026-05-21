package com.kinetica.nifi.processors;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;

import com.kinetica.nifi.processors.base.AbstractGetKineticaProcessor;

/**
 * NiFi processor that monitors a Kinetica table and outputs new records as JSON files.
 *
 * <p>This processor uses Kinetica's table monitor feature via ZeroMQ to receive
 * notifications of new records and outputs them as JSON-formatted FlowFiles.
 *
 * <p>Key features:
 * <ul>
 *   <li>Real-time monitoring via ZeroMQ subscription</li>
 *   <li>JSON output using Avro JSON encoding</li>
 *   <li>Proper resource cleanup (ZeroMQ connection leak fixed)</li>
 * </ul>
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "get", "json", "monitor", "stream"})
@CapabilityDescription("Monitors a Kinetica table for new records and outputs them as JSON. " +
        "Uses ZeroMQ table monitor for real-time notifications. " +
        "Records are output as JSON-encoded Avro records.")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to application/json")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
public class GetKineticaToJSON extends AbstractGetKineticaProcessor {

    private static final String PROCESSOR_NAME = "GetKineticaToJSON";

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

        final int recordCount = records.size();

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
                    // Write records as JSON array
                    writer.write("[");

                    boolean first = true;
                    for (GenericRecord record : records) {
                        if (!first) {
                            writer.write(",");
                        }
                        first = false;

                        // Use Avro JSON encoder
                        writeRecordAsJson(record, out);
                    }

                    writer.write("]");
                    writer.flush();
                }
            }
        });

        // Set attributes
        Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
        attributes.put(CoreAttributes.FILENAME.key(),
                flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
        attributes.put("record.count", String.valueOf(recordCount));
        attributes.put("kinetica.table", tableName);
        flowFile = session.putAllAttributes(flowFile, attributes);

        // Transfer to success
        session.getProvenanceReporter().receive(flowFile, gpudb.getURL().toString(), tableName);
        session.transfer(flowFile, REL_SUCCESS);

        getLogger().info("{}: Output {} records from table '{}' as JSON",
                PROCESSOR_NAME, recordCount, tableName);
    }

    /**
     * Writes a GenericRecord as JSON using Avro's JSON encoder.
     */
    private void writeRecordAsJson(GenericRecord record, OutputStream out) throws IOException {
        Schema schema = record.getSchema();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out, false);
        DatumWriter<Object> datumWriter = new GenericDatumWriter<>(schema);
        datumWriter.write(record, encoder);
        encoder.flush();
    }
}
