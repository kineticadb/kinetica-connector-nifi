package com.kinetica.nifi.processors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.Type.Column;
import com.kinetica.nifi.processors.base.AbstractPutKineticaProcessor;
// Note: KineticaUtilities methods are now accessed via base class setColumnValue()

/**
 * NiFi processor that bulk loads FlowFile attributes to Kinetica.
 *
 * <p>This processor reads FlowFile attributes and inserts them as records into a Kinetica table.
 * Each FlowFile's attributes should match the column names defined in the table schema.
 *
 * <p>Key features:
 * <ul>
 *   <li>Batch insertion for high throughput</li>
 *   <li>Automatic table creation from schema definition</li>
 *   <li>Primary key update support</li>
 *   <li>Timestamp/date parsing</li>
 * </ul>
 *
 * <p>Example: For a table with columns (x, y, timestamp), each FlowFile should have
 * attributes named "x", "y", and "timestamp" with appropriate values.
 *
 * @author Kinetica Engineering
 * @version 7.2.0.0
 * @since 7.2.0.0
 */
@Tags({"Kinetica", "add", "bulkadd", "put", "insert"})
@CapabilityDescription("Bulk loads FlowFile attributes to Kinetica in batch intervals. " +
        "Each FlowFile must contain attributes that match your schema definition. " +
        "Example: Given schema 'x|Float|data,y|Float|data,TEXT|String|data', " +
        "this processor expects attributes named x, y, and TEXT in each FlowFile. " +
        "Set Batch Size appropriately for your throughput needs (e.g., 10K records/sec requires batch size ~10000).")
@ReadsAttribute(attribute = "*", description = "Reads all FlowFile attributes that match the table column names")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class PutKinetica extends AbstractPutKineticaProcessor {

    private static final String PROCESSOR_NAME = "PutKinetica";

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get FlowFiles up to the batch size
        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.isEmpty()) {
            return;
        }

        getLogger().debug("{}: Processing {} FlowFiles", PROCESSOR_NAME, flowFiles.size());

        // Track successful FlowFiles
        List<FlowFile> successes = new ArrayList<>();
        BulkInserter<Record> bulkInserter = null;

        try {
            // Create bulk inserter
            bulkInserter = createBulkInserter();

            final long startTime = System.currentTimeMillis();

            // Process each FlowFile
            for (final FlowFile flowFile : flowFiles) {
                try {
                    Record record = createRecordFromFlowFile(flowFile);
                    if (record != null) {
                        bulkInserter.insert(record);
                        successes.add(flowFile);
                    } else {
                        getLogger().error("{}: Failed to create record from FlowFile {}",
                                PROCESSOR_NAME, flowFile.getId());
                        session.transfer(flowFile, REL_FAILURE);
                    }
                } catch (BulkInserter.InsertException e) {
                    getLogger().error("{}: Insert error for FlowFile {}: {}",
                            PROCESSOR_NAME, flowFile.getId(), e.getMessage());
                    session.transfer(flowFile, REL_FAILURE);
                }
            }

            // Flush remaining records
            bulkInserter.flush();

            final long duration = System.currentTimeMillis() - startTime;

            // Transfer successful FlowFiles
            for (FlowFile flowFile : successes) {
                session.transfer(flowFile, REL_SUCCESS);
                session.getProvenanceReporter().send(flowFile,
                        gpudb.getURL().toString() + "/" + tableName,
                        "Inserted into Kinetica table " + tableName,
                        duration);
            }

            getLogger().info("{}: Inserted {} records to table '{}' in {}ms",
                    PROCESSOR_NAME, successes.size(), tableName, duration);

        } catch (BulkInserter.InsertException e) {
            getLogger().error("{}: Flush error: {}", PROCESSOR_NAME, e.getMessage(), e);
            // Transfer all FlowFiles that were added to successes to failure since flush failed
            for (FlowFile flowFile : successes) {
                session.transfer(flowFile, REL_FAILURE);
            }
        } catch (GPUdbException e) {
            getLogger().error("{}: Failed to create bulk inserter: {}", PROCESSOR_NAME, e.getMessage(), e);
            // Transfer all FlowFiles to failure
            for (FlowFile flowFile : flowFiles) {
                if (!successes.contains(flowFile)) {
                    session.transfer(flowFile, REL_FAILURE);
                }
            }
            // Also transfer successes to failure since we couldn't insert them
            for (FlowFile flowFile : successes) {
                session.transfer(flowFile, REL_FAILURE);
            }
        }
    }

    /**
     * Creates a Kinetica Record from FlowFile attributes.
     *
     * @param flowFile The FlowFile to read attributes from
     * @return A Record populated with values from attributes, or null on error
     */
    private Record createRecordFromFlowFile(FlowFile flowFile) {
        Record record = createEmptyRecord();
        if (record == null) {
            return null;
        }

        Map<String, String> attributes = flowFile.getAttributes();

        for (Column column : objectType.getColumns()) {
            String value = attributes.get(column.getName());
            if (!setColumnValue(record, column, value)) {
                return null;
            }
        }

        return record;
    }
}
