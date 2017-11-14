package com.gisfederal.gpudb.processors.GPUdbNiFi;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.gpudb.BulkInserter;
import com.gpudb.GPUdb;
import com.gpudb.GPUdbBase.Options;
import com.gpudb.GPUdbException;
import com.gpudb.Record;
import com.gpudb.RecordObject;
import com.gpudb.Type;
import com.gpudb.Type.Column;
import com.gpudb.protocol.CreateTableRequest;
import com.gpudb.protocol.HasTableResponse;
import com.gpudb.protocol.InsertRecordsRequest;

@Tags({ "Kinetica", "add", "bulkadd", "put", "csv", "delimited", "file" })
@CapabilityDescription("Bulkloads the contents of a delimited file (tab, comma, pipe, etc) to Kinetica. Each file must contain the exact columns as defined in the Schema definition. "
		+ "Example: Given this schema: x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search,AUTHOR|String|text_search|data, "
		+ "this processor would expect columns of x, y, TIMESTAMP, TEXT and AUTHOR in the same order in the file (null or blank values are okay). "
		+ "This processor will ignore the header record of the file. For best results, chunk your file in to 1M rows at a time, so NiFi "
		+ "does not hit memory issues parsing the file. Additionally, Nifi runs better if you adjust Concurrent tasks and Run schedule. Example: "
		+ " Concurrent tasks to 2 and Run schedule to 2 sec on the Scheduling tab.")
@ReadsAttribute(attribute = "mime.type", description = "Determines MIME type of input file")
public class PutKineticaFromFile extends AbstractProcessor {
	public static final PropertyDescriptor PROP_SERVER = new PropertyDescriptor.Builder().name("Server URL")
			.description("URL of the Kinetica server. Example http://172.3.4.19:9191").required(true)
			.addValidator(StandardValidators.URL_VALIDATOR).build();

	public static final PropertyDescriptor PROP_COLLECTION = new PropertyDescriptor.Builder().name("Collection Name")
			.description("Name of the Kinetica collection").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PROP_TABLE = new PropertyDescriptor.Builder().name("Table Name")
			.description("Name of the Kinetica table").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PROP_SCHEMA = new PropertyDescriptor.Builder().name("Schema")
			.description("Schema of the Kinetica table. Schema not required if table exists in Kinetica already."
					+ " Example schema: x|Float|data,y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search,AUTHOR|String|text_search|data")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor.Builder().name("Delimiter")
			.description("Delimiter of CSV input data (usually a ',' or '\t' (tab); defaults to '\t' (tab))")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue("\t").build();

	protected static final PropertyDescriptor PROP_BATCH_SIZE = new PropertyDescriptor.Builder().name("Batch Size")
			.description("Batch size of bulk load to Kinetica.").required(true)
			.addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR).defaultValue("500").build();

	protected static final PropertyDescriptor PROP_ERROR_HANDLING = new PropertyDescriptor.Builder()
			.name("Error Handling")
			.description(
					"Value of true means skip any errors and keep processing. Value of false means stop all processing when an error "
							+ "occurs in a file.")
			.required(true).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("true").build();

	public static final PropertyDescriptor PROP_USERNAME = new PropertyDescriptor.Builder().name("Username")
			.description("Username to connect to Kinetica").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PROP_PASSWORD = new PropertyDescriptor.Builder().name("Password")
			.description("Password to connect to Kinetica").required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();

	protected static final PropertyDescriptor UPDATE_ON_EXISTING_PK = new PropertyDescriptor.Builder()
			.name("Update on Existing PK")
			.description(
					"If the table has a primary key, then if the value is 'true' then if any of the records being added have the "
							+ "same primary key as existing records, the existing records are replaced (i.e. *updated*) with the given records. "
							+ "If 'false' and if the records being added have the same primary key as existing records, the given records with "
							+ "existing primary keys are ignored (the existing records are left unchanged). It is quite possible that in this "
							+ "case some of the given records will be inserted and some (those having existing primary keys) will be ignored "
							+ "(or updated). If the specified table does not have a primary key column then this parameter is ignored. ")
			.required(true).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("false").build();

	protected static final PropertyDescriptor PROP_REPLICATE_TABLE = new PropertyDescriptor.Builder()
			.name("Replicate Table")
			.description(
					"If the Kinetica table doesn't already exist then it will created by this processor. A value of true indicates that"
							+ " the table that is created should be replicated.")
			.required(true).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("false").build();

	public static final PropertyDescriptor PROP_DATE_FORMAT = new PropertyDescriptor.Builder().name("Date Format")
			.description("Provide the date format used for your datetime values"
					+ " Example: yyyy/MM/dd HH:mm:ss")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final PropertyDescriptor PROP_TIMEZONE = new PropertyDescriptor.Builder().name("Timezone")
			.description(
					"Provide the timezone the data was created in. If no timezone is set, the current timezone will be used."
							+ " Example: EST")
			.required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("All FlowFiles that are written to Kinetica are routed to this relationship").build();

	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("All FlowFiles that cannot be written to Kinetica are routed to this relationship").build();

	private GPUdb gpudb;
	private String tableName;
	public Type objectType;
	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;
	private char delimiter;
	private boolean updateOnExistingPk;
	private String dateFormat;
	private String timeZone;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<>();
		descriptors.add(PROP_SERVER);
		descriptors.add(PROP_COLLECTION);
		descriptors.add(PROP_TABLE);
		descriptors.add(PROP_SCHEMA);
		descriptors.add(PROP_DELIMITER);
		descriptors.add(PROP_BATCH_SIZE);
		descriptors.add(PROP_ERROR_HANDLING);
		descriptors.add(PROP_USERNAME);
		descriptors.add(PROP_PASSWORD);
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
		getLogger().info("Kinetica-createTable:" + tableName + ", schemaStr:" + schemaStr);
		HasTableResponse response = gpudb.hasTable(tableName, null);
		if (response.getTableExists()) {
			return (null);
		}
		List<Column> attributes = new ArrayList<>();
		int maxPrimaryKey = -1;
		String[] fieldArray = schemaStr.split(",");
		for (String fieldStr : fieldArray) {
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
		getLogger().info("Kinetica-type:" + attributes);
		Type type = new Type("", attributes);

		String typeId = type.create(gpudb);
		response = gpudb.hasTable(tableName, null);
		Map<String, String> create_table_options;
		String parent = context.getProperty(PROP_COLLECTION).getValue();
		if (parent == null) {
			parent = "";
		}

		if (!response.getTableExists()) {
			boolean replicated_flag = context.getProperty(PROP_REPLICATE_TABLE).isSet()
					&& context.getProperty(PROP_REPLICATE_TABLE).asBoolean().booleanValue();
			getLogger().debug("replicated_flag = " + replicated_flag);

			create_table_options = GPUdb.options(CreateTableRequest.Options.COLLECTION_NAME, parent,
					CreateTableRequest.Options.IS_REPLICATED,
					replicated_flag ? CreateTableRequest.Options.TRUE : CreateTableRequest.Options.FALSE);

			getLogger().debug("create_table_options has " + create_table_options.size() + "properties");
			gpudb.createTable(context.getProperty(PROP_TABLE).getValue(), typeId, create_table_options);
		}

		gpudb.addKnownType(typeId, RecordObject.class);
		return type;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) throws GPUdbException {
		Options option = new Options();
		if (context.getProperty(PROP_USERNAME).getValue() != null
				&& context.getProperty(PROP_PASSWORD).getValue() != null) {
			option.setUsername(context.getProperty(PROP_USERNAME).getValue());
			option.setPassword(context.getProperty(PROP_PASSWORD).getValue());
		}
		gpudb = new GPUdb(context.getProperty(PROP_SERVER).getValue(), option);
		tableName = context.getProperty(PROP_TABLE).getValue();
		delimiter = context.getProperty(PROP_DELIMITER).getValue().charAt(0);
		updateOnExistingPk = context.getProperty(UPDATE_ON_EXISTING_PK).asBoolean().booleanValue();
		dateFormat = context.getProperty(PROP_DATE_FORMAT).getValue();
		timeZone = context.getProperty(PROP_TIMEZONE).getValue();

		if (KineticaUtilities.tableExists(gpudb, tableName, getLogger())) {
			getLogger().debug("Getting type from table:" + tableName);
			objectType = Type.fromTable(gpudb, tableName);
			getLogger().debug("objectType:" + objectType.toString());
		} else if (context.getProperty(PROP_SCHEMA).isSet()) {
			objectType = createTable(context, context.getProperty(PROP_SCHEMA).getValue());
		} else {
			objectType = null;
		}
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final FlowFile flowFile = session.get();
		final int batchSize = context.getProperty(PROP_BATCH_SIZE).asInteger();
		final boolean skipErrors = context.getProperty(PROP_ERROR_HANDLING).asBoolean();
		final BulkInserter<Record> bulkInserter;
		final BulkInserter.WorkerList workers;

		if (flowFile == null) {
			return;
		}
		
		try {
			if (!KineticaUtilities.tableExists(gpudb, tableName, getLogger())) {
				throw new ProcessException("Kinetica Error: The Table: " + tableName + " does not exist in Kinetica. Please provide a schema or create"
						+ " the table prior to loading data." );
			}
			workers = new BulkInserter.WorkerList(gpudb);
			bulkInserter = new BulkInserter<Record>(gpudb, tableName, objectType, batchSize, GPUdb.options(
					InsertRecordsRequest.Options.UPDATE_ON_EXISTING_PK,
					updateOnExistingPk ? InsertRecordsRequest.Options.TRUE : InsertRecordsRequest.Options.FALSE),
					workers);
		} catch (Exception e) {
			throw new ProcessException("Failed to create BulkInserter " + KineticaUtilities.convertStacktraceToString(e));
		}
		
		// Note: The following are length 1 arrays so that they can be declared
		// final and they can be used in anonymous functions.

		final Type[] type = new Type[1];
		final int[][] attributeNumbers = new int[1][];
		final boolean[] failed = { false };

		session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(InputStream in) throws IOException {
				CSVParser parser = null;
				try {	
					parser = new CSVParser(new InputStreamReader(in),
							CSVFormat.DEFAULT.withDelimiter(delimiter));
					
					type[0] = objectType;

					if (type[0] == null) {
						type[0] = createTable(context, context.getProperty(PROP_SCHEMA).getValue());
						objectType = type[0];
					}
					
					int count = 0;
					int errorCount = 0;
					List<CSVRecord> recordList = parser.getRecords();
					
					Type tempType = objectType;
					for (CSVRecord record : recordList) {
						boolean failed = false;
						if (record.size() != type[0].getColumnCount()) {
							// if we are not skipping errors, reject the whole
							// file
							if (!skipErrors) {
								throw new ProcessException("Error in record " + (count + 1)
										+ ": Incorrect number of fields. " + record.toString());
							} else {
								// if we are skipping errors, jump to next row
								continue;
							}
						}
						
						Record object = tempType.newInstance();
						attributeNumbers[0] = new int[record.size()];
						
						for (int i = 0; i < record.size(); i++) {
							int attributeNumber = attributeNumbers[0][i];

							if (attributeNumber > -1) {
								String value = record.get(i);
								Column attribute = type[0].getColumn(i);
								if (value.trim().length() == 0) {
									value = null;
								}
								
								try {
									boolean timeStamp = KineticaUtilities.checkForTimeStamp(attribute.getProperties());
									if (timeStamp && value != null) {
										if (StringUtils.isNumeric(value)) {
											long valueLong;
											try {
												valueLong = Long.parseLong(value);
											} catch (NumberFormatException ex) {
												valueLong = 0;
											}
	
											object.put(attribute.getName(), valueLong);
										} else {
	
											Long timestamp = KineticaUtilities.parseDate(value, dateFormat, timeZone, getLogger());
											
											if (timestamp != null) {
												object.put(attribute.getName(), timestamp);
											} else {		
												getLogger().error("Failed to parse date. Please check your date format and try again.");
												failed = true;
												break;
											}
										}
									} else if (attribute.getType() == Double.class && value != null) {
										object.put(attribute.getName(), Double.parseDouble(value));
									} else if (attribute.getType() == Float.class && value != null) {
										object.put(attribute.getName(), Float.parseFloat(value));
									} else if (attribute.getType() == Integer.class && value != null) {
										object.put(attribute.getName(), Integer.parseInt(value));
									} else if (attribute.getType() == java.lang.Long.class && value != null) {
										object.put(attribute.getName(), Long.parseLong(value));
									} else {
										if (value != null && !value.trim().equals("")) {
											object.put(attribute.getName(), value.trim());
										}
									}
								} catch (Exception e) {
									// if we are not skipping errors, reject the
									// whole file
									if (!skipErrors) {
										session.transfer(flowFile, REL_FAILURE);
										throw new ProcessException("Error in record " + (count + 1) + ": Invalid value \""
												+ value + "\" for field " + attribute.getName() + ".");
									} else {
										// if we are skipping errors, jump to
										// next record
										/*
										 * Todo - put all errors in a new file
										 * that can be looked at by the user so
										 * they can make corrections
										 */
										errorCount++;
										getLogger().warn("Error in record " + (count + 1) + ": Invalid value \""
												+ value + "\" for field " + attribute.getName() + ". Total error count = " + errorCount);
										failed = true;
										break;
									}
								}
							}
						}

						if (!failed) {
							try {
								bulkInserter.insert(object);
							} catch (BulkInserter.InsertException e) {
								getLogger().error(KineticaUtilities.convertStacktraceToString(e));
							}
						}
						count++;
					}

					// Flush the bulk inserter object to make sure all objects
					// are inserted
					try {
						bulkInserter.flush();
					} catch (BulkInserter.InsertException e) {
						getLogger().error(KineticaUtilities.convertStacktraceToString(e));
					}

					getLogger().info("Wrote {} record(s) to set {} at {}.",
							new Object[] { count, tableName, gpudb.getURL() });
				} catch (Exception ex) {
					getLogger().error("Failed to write to set {} at {} in second read",
							new Object[] { tableName, gpudb.getURL() }, ex);
					failed[0] = true;
				} finally {
					parser.close();
				}
			}
		});

		if (failed[0]) {
			session.transfer(flowFile, REL_FAILURE);
		} else {
			session.getProvenanceReporter().send(flowFile, gpudb.getURL().toString(), tableName);
			session.transfer(flowFile, REL_SUCCESS);
		}
	}
}
