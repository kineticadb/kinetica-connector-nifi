package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

@Tags({"gpudb", "convert", "csv", "json", "tweet", "twitter"})
@CapabilityDescription("Converts a JSON tweet object into a GPUdb-compatible CSV file")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to text/csv")
public class ConvertTweetToCSV extends AbstractProcessor {
	public static final char DELIMITER = '\t';
	
    public static final Relationship REL_WITH_COORDS = new Relationship.Builder()
            .name("with coordinates")
            .description("All converted tweets with coordinates are routed to this relationship")
            .build();

    public static final Relationship REL_WITHOUT_COORDS = new Relationship.Builder()
            .name("without coordinates")
            .description("All converted tweets without coordinates are routed to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All invalid or non-tweets are routed unchanged to this relationship")
            .build();

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_WITH_COORDS);
        relationships.add(REL_WITHOUT_COORDS);
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

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }

        final Map<String, Object> attributes = new HashMap<>();

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {               
                try {
                    // getLogger().info("ConvertTweetToCSV, in read, in process, in try");
                    java.util.Scanner s = new java.util.Scanner(in).useDelimiter("\\A");
                    String tweet = s.next();
                    DocumentContext dc = JsonPath.parse(tweet);

                    String id = dc.read("$.id_str");
                    attributes.put("author", dc.<String>read("$.user.screen_name"));
                    attributes.put("text", dc.<String>read("$.text"));

                    long timestamp = dateFormat.parse(dc.<String>read("$.created_at")).getTime();
                    long now = new java.util.Date().getTime();
                    
                    if ((timestamp < 1143006600000L) || (timestamp > now  + 24 * 60 * 60 * 1000)) {
                        throw new Exception("Invalid timestamp:" + timestamp);
                    }

                    attributes.put("timestamp", timestamp);
                    attributes.put("url", "http://twitter.com/" + (String)attributes.get("author") + "/statuses/" + id);

                    try {
                        attributes.put("x", dc.<Double>read("$.coordinates.coordinates[0]"));
                        attributes.put("y", dc.<Double>read("$.coordinates.coordinates[1]"));
                    } catch (Exception ex) {
                    }
                } catch (Exception ex) {
                    getLogger().info("ConvertTweetToCSV, in read, in process, in catch, exception:" + ex.getMessage());
                    attributes.clear();
                    attributes.put("error", ex.getMessage());
                }
            }
        });

        if (attributes.containsKey("error")) {
            session.transfer(flowFile, REL_FAILURE);
        } else {
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
                        CSVPrinter printer = new CSVPrinter(writer, CSVFormat.RFC4180.withDelimiter(DELIMITER));
                        if (attributes.containsKey("y")) {
                            printer.printRecord(
                                    "TIMESTAMP|long",
                                    "x|float",
                                    "y|float",
                                    "AUTHOR|string|data|text_search",
                                    "URL|string|store_only",
                                    "TEXT|string|store_only|text_search",
		                            "SENTIMENT_VADER_P|float|data",
		                            "SENTIMENT_TXTBLOB_P|float|data",
		                            "SENTIMENT_AFIN_WO_EMO_P|float|data",
		                            "SENTIMENT_AFIN_W_EMO_P|float|data",
		                            "SENTIMENT_EMOJI_J|int|data",
		                            "SENTIMENT_LGCL_RGRES_J|int|data",
		                            "SENTIMENT_DICT_J|int|data");

                            printer.printRecord(
                                    attributes.get("timestamp"),
                                    attributes.get("x"),
                                    attributes.get("y"),
                                    attributes.get("author"),
                                    attributes.get("url"),
                                    attributes.get("text"),
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0);
                        } else {
                            printer.printRecord(
                                    "TIMESTAMP|long",
                                    "AUTHOR|string|data|text_search",
                                    "URL|string|store_only",
                                    "TEXT|string|store_only|text_search",
		                            "SENTIMENT_VADER_P|float|data",
		                            "SENTIMENT_TXTBLOB_P|float|data",
		                            "SENTIMENT_AFIN_WO_EMO_P|float|data",
		                            "SENTIMENT_AFIN_W_EMO_P|float|data",
		                            "SENTIMENT_EMOJI_J|int|data",
		                            "SENTIMENT_LGCL_RGRES_J|int|data",
		                            "SENTIMENT_DICT_J|int|data");
                            
                            printer.printRecord(
                                    attributes.get("timestamp"),
                                    attributes.get("author"),
                                    attributes.get("url"),
                                    attributes.get("text"),
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0,
                                    0);
                        }

                        printer.flush();
                        writer.flush();
                    }
                    catch(Exception e){
                        getLogger().info("failed try");
                        getLogger().info(e.getMessage());
                    }
                }
            });

            flowFile = session.putAttribute(flowFile, CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".csv");
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "text/csv");
            session.getProvenanceReporter().modifyContent(flowFile);

            if (attributes.containsKey("y")) {
                getLogger().info("transferring flow file with coords:"+ flowFile.toString());
                session.transfer(flowFile, REL_WITH_COORDS);
            } else {
                session.transfer(flowFile, REL_WITHOUT_COORDS);
            }
        }
    }
}