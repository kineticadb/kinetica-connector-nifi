package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.gpudb.GPUdb;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableResponse;


import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
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
public class PutWordCloudGPUdb extends AbstractProcessor {
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

    
    public static final PropertyDescriptor DICTIONARY_FILE = new PropertyDescriptor.Builder()
            .name("Dictionary File")
            .description("Path/filename of dictionary file")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor STOP_WORDS = new PropertyDescriptor.Builder()
            .name("Stop words")
            .description("Stop words")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    
    public static final PropertyDescriptor EXTRA_WORDS = new PropertyDescriptor.Builder()
            .name("Extra words")
            .description("Extra words")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
        
    public static final PropertyDescriptor MAXIMUM_WORDS = new PropertyDescriptor.Builder()
            .name("Maximum words")
            .description("Maximum number words")
            .required(false)
            .defaultValue("100")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
 
    public static final PropertyDescriptor MINIMUM_WORD_LENGTH = new PropertyDescriptor.Builder()
            .name("Minimum word length")
            .description("Minimum word length")
            .required(false)
            .defaultValue("2")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
    private ComponentLog logger;
    private char delimiter;
    private List<String> dictionary = null;
    private List<String> stopWords  = null;
    private List<String> extraWords = new ArrayList<String>();
    private Integer maxWords = 100;
    private Integer minWordLength = 2;
    

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_SERVER);
        descriptors.add(PROP_PARENT_SET);
        descriptors.add(PROP_SET);
        descriptors.add(PROP_LABEL);
        descriptors.add(PROP_DELIMITER);
        descriptors.add(PROP_SEMANTIC_TYPE);
        descriptors.add(PROP_VERSION);
        descriptors.add(DICTIONARY_FILE);
        descriptors.add(STOP_WORDS);
        descriptors.add(EXTRA_WORDS);        
        descriptors.add(MAXIMUM_WORDS);        
        descriptors.add(MINIMUM_WORD_LENGTH);        
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

    private Type createSet(ProcessContext context) throws GPUdbException {
        List<Column> attributes = new ArrayList<>();
        
        List<String> annotations = new ArrayList<>();        
        attributes.add(new Column("x", Float.class, annotations));
        attributes.add(new Column("y", Float.class, annotations));
        attributes.add(new Column("TIMESTAMP", Long.class, annotations));
        annotations.clear();
        annotations.add("char16");
        attributes.add(new Column("word", String.class, annotations));
        annotations.clear();
        annotations.add("store_only");
        annotations.add("text_search");
        attributes.add(new Column("TEXT", String.class, annotations));
        annotations.clear();
        annotations.add("store_only");
        attributes.add(new Column("URL", String.class, annotations));
        
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
        if(this.dictionary==null){
            File file = new File(context.getProperty(DICTIONARY_FILE).getValue());
            try {
                this.dictionary = FileUtils.readLines(file);
            } catch (IOException ex) {
                Logger.getLogger(PutWordCloudGPUdb.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        if(this.stopWords==null){
            String [] items = context.getProperty(STOP_WORDS).getValue().split(",");
            this.stopWords = Arrays.asList(items);            
        }
        if(this.extraWords.isEmpty()){
            String extraWordsStr = context.getProperty(EXTRA_WORDS).getValue();
            if((extraWordsStr!=null)&&(!extraWordsStr.isEmpty())){
                String [] items = context.getProperty(EXTRA_WORDS).getValue().split(",");
                this.extraWords = Arrays.asList(items);            
            }
        }
        this.maxWords      = Integer.parseInt(context.getProperty(MAXIMUM_WORDS).getValue());
        this.minWordLength = Integer.parseInt(context.getProperty(MINIMUM_WORD_LENGTH).getValue());
        this.gpudb         = new GPUdb(context.getProperty(PROP_SERVER).getValue());
        this.set           = context.getProperty(PROP_SET).getValue();
        this.delimiter     = context.getProperty(PROP_DELIMITER).getValue().charAt(0);
        this.objectType    = createSet(context);
       
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
            public void process(InputStream in) throws IOException {
                try {
                    InputStreamReader inputReader = new InputStreamReader(in);
                    CSVParser parser = new CSVParser(inputReader, CSVFormat.RFC4180.withDelimiter(delimiter));
                    parser.iterator().next();
                    int count = 0;
                    for (CSVRecord record : parser) {  
                        long timestamp  = Long.parseLong(record.get(0)); 
                        float  x        = Float.parseFloat(record.get(1));
                        float  y        = Float.parseFloat(record.get(2));
                        String url      = record.get(4);
                        String text     = record.get(5);
                        List<Record> records = parse(x,y,timestamp,url,text);
                        if(!records.isEmpty()){
                            gpudb.insertRecords(set, records, null);
                            getLogger().info("Wrote {} record(s) to set {} at {}.", new Object[] { count, set, gpudb.getURL() });
                        }
                    }
                } catch (Exception ex) {
                    getLogger().error("Failed to write to set {} at {}", new Object[] { set, gpudb.getURL() }, ex);
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
    
    
	private List<String> getWords(String text) {
            List<String> words = new ArrayList<>(20);

            for (String word : text.split("[ \t\r\n]")) {
                word = word.toLowerCase();

                if (word.startsWith("#")) {
                    words.add(word);
                    continue;
                }

                int start = 0;

                while (start < word.length()) {
                    char c = word.charAt(start);

                    if (c >= 'a' && c <= 'z') {
                        break;
                    }

                    start++;
                }

                if (start == word.length()) {
                    continue;
                }

                int end = word.length() - 1;

                while (end >= start) {
                    char c = word.charAt(end);

                    if (c >= 'a' && c <= 'z') {
                        break;
                    }

                    end--;
                }

                if (end < start) {
                    continue;
                }

                word = word.substring(start, end + 1);

                if (!extraWords.contains(word)) {
                    if (word.length() < minWordLength) {
                        continue;
                    }

                    if (stopWords.contains(word)) {
                        continue;
                    }

                    if (!this.dictionary.contains(word)) {
                        continue;
                    }
                }

                words.add(word);
            }

            Collections.sort(words, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return Integer.compare(o2.length(), o1.length());
                }
            });
            return words;
	}


	public List<Record> parse(float x, float y, long timestamp,String url, String text) {
            try {
                List<Record> results = new ArrayList<>(this.maxWords);
                List<String> words = getWords(text);

                for (int i = 0; i < words.size() && i < maxWords; i++) {
                    Record object = Type.fromTable(gpudb, set).newInstance();
                    object.put("x", x);
                    object.put("y", y);
                    object.put("TIMESTAMP", timestamp);

                    String word = words.get(i);

                    if (word.length() > 16) {
                            object.put("word", word.substring(0, 16));
                    } else {
                            object.put("word", word);
                    }

                    object.put("TEXT", text);
                    object.put("URL", url);
                    results.add(object);
                }
                return results;
            } catch (Exception ex) {
                getLogger().error("Failed to write to set {} at {} in first read", new Object[] { set, gpudb.getURL() }, ex);
                return null;
            }
	}
    
    
    
}