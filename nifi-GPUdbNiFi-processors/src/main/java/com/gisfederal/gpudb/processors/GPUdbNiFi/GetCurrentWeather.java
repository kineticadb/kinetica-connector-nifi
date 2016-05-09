package com.gisfederal.gpudb.processors.GPUdbNiFi;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.OutputStreamCallback;

@Tags({"gpudb", "csv", "get", "openweathermap", "weather"})
@CapabilityDescription("Gets OpenWeatherMap current weather data into GPUdb-compatible CSV files")
@WritesAttribute(attribute = "mime.type", description = "Sets MIME type to text/csv")
public class GetCurrentWeather extends AbstractProcessor {
    public static final PropertyDescriptor PROP_API_KEY = new PropertyDescriptor.Builder()
            .name("API Key")
            .description("OpenWeatherMap API key")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_CITY_FILE = new PropertyDescriptor.Builder()
            .name("City File")
            .description("File containing city list")
            .required(false)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_THREAD_COUNT = new PropertyDescriptor.Builder()
            .name("Thread Count")
            .description("Number of threads to use for collecting weather data")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("1")
            .build();

    public static final PropertyDescriptor PROP_REFRESH_INTERVAL = new PropertyDescriptor.Builder()
            .name("Refresh Interval")
            .description("Time (ms) to wait between refresh attempts")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("600000")
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All OpenWeatherMap CSV files are routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROP_API_KEY);
        descriptors.add(PROP_CITY_FILE);
        descriptors.add(PROP_THREAD_COUNT);
        descriptors.add(PROP_REFRESH_INTERVAL);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
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

    private Thread mainThread;
    private ConcurrentLinkedQueue<Object> queue;


    private static List<Object> currentWeatherForCities(String apiKey, List<Integer> cities, ProcessorLog logger) throws IOException {
        HttpURLConnection connection = null;
        BufferedReader reader = null;

        try {
            StringBuilder builder = new StringBuilder();

            for (int city : cities) {
                if (builder.length() > 0) {
                    builder.append(',');
                }

                builder.append(city);
            }

            connection = (HttpURLConnection)new URL(
                    "http://api.openweathermap.org/data/2.5/group"
                            + "?id=" + builder
                            + "&units=imperial"
                            + "&lang=en"
                            + (apiKey != null ? "&appid=" + apiKey : "")).openConnection();
            connection.setConnectTimeout(60000);
            connection.setReadTimeout(60000);
            connection.setRequestMethod("GET");
            connection.setUseCaches(false);
            connection.setDoInput(true);
            connection.setDoOutput(false);
            connection.setRequestProperty("Accept-Encoding", "gzip, deflate");
            connection.connect();

            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                String encoding = connection.getContentEncoding();

                if (encoding != null && "gzip".equalsIgnoreCase(encoding)) {
                    reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(connection.getInputStream())));
                } else if (encoding != null && "deflate".equalsIgnoreCase(encoding)) {
                    reader = new BufferedReader(new InputStreamReader(new InflaterInputStream(connection.getInputStream(), new Inflater(true))));
                } else {
                    reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                }

                String line;
                String response = null;

                while ((line = reader.readLine()) != null) {
                    response = line;
                }

                return JsonPath.read(response, "$.list");
            } else {
                throw new IOException(connection.getResponseCode() + " " + connection.getResponseMessage());
            }
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }

                if (connection != null) {
                    connection.disconnect();
                }
            } catch (Exception ex) {
            }
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) throws IOException {
        final List<Integer> cities = new ArrayList<>();

        BufferedReader cityReader;

        if (context.getProperty(PROP_CITY_FILE).isSet()) {
            cityReader = new BufferedReader(new FileReader(context.getProperty(PROP_CITY_FILE).getValue()));
        } else {
            cityReader = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream("/city.list.json")));
        }

        String line;

        while ((line = cityReader.readLine()) != null) {
            DocumentContext dc = JsonPath.parse(line);

            try {
                cities.add(dc.<Integer>read("$._id"));
            } catch (Exception ex) {
                getLogger().warn("Bad city record", ex);
            }
        }

        cityReader.close();

        final ExecutorService executor = Executors.newFixedThreadPool(context.getProperty(PROP_THREAD_COUNT).asInteger());
        queue = new ConcurrentLinkedQueue<>();
        final Map<Integer, Long> weatherTimes = new ConcurrentHashMap<>();

        mainThread = new Thread(new Runnable() {
            @Override
            public void run() {
                // Note: The following is a length 1 array so that it can be
                // declared final and it can be used in anonymous functions.

                final boolean[] run = new boolean[] { true };

                while (run[0]) {
                    if (Thread.interrupted()) {
                        run[0] = false;
                        break;
                    }

                    long start = new Date().getTime();
                    List<Future<?>> futures = new ArrayList<>();

                    for (int i = 0; i < cities.size(); i += 16) {
                        if (Thread.interrupted()) {
                            run[0] = false;
                            break;
                        }

                        final List<Integer> cityList = new ArrayList<>();

                        for (int j = i; j < i + 16 && j < cities.size(); j++) {
                            cityList.add(cities.get(j));
                        }

                        futures.add(executor.submit(new Runnable() {
                            @Override
                            public void run() {
                                if (!run[0]) {
                                    return;
                                }

                                try {
                                    List<Object> weatherList = currentWeatherForCities(context.getProperty(PROP_API_KEY).getValue(), cityList, getLogger());
                                    for (Object weather : weatherList) {
                                        ReadContext rc = JsonPath.parse(weather);
                                        int city = rc.read("$.id", Integer.class);
                                        long dt = rc.read("$.dt", Long.class);
                                        long lastTime=0;
                                        if(weatherTimes.size()>=city){
                                            lastTime = weatherTimes.get(city);
                                        }
                                                //getOrDefault(city, 0L);

                                        if (dt > lastTime) {
                                            queue.add(weather);
                                            weatherTimes.put(city, dt);
                                        }
                                    }
                                } catch (Exception ex) {
                                    getLogger().error("Unable to get weather data", ex);
                                }
                            }
                        }));
                    }

                    for (Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (InterruptedException ex) {
                            run[0] = false;
                            break;
                        } catch (ExecutionException ex) {
                            getLogger().error("Unable to get weather data", ex);
                        }
                    }

                    long stop = new Date().getTime();

                    if (stop - start < context.getProperty(PROP_REFRESH_INTERVAL).asInteger()) {
                        try {
                            Thread.sleep(context.getProperty(PROP_REFRESH_INTERVAL).asInteger() - (stop - start));
                        } catch (InterruptedException ex) {
                            run[0] = false;
                            break;
                        }
                    }
                }

                executor.shutdownNow();
            }
        });

        mainThread.start();
    }

    @OnUnscheduled
    public void onUnscheduled(final ProcessContext context) {
        if (mainThread != null) {
            mainThread.interrupt();
            mainThread = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<Object> weatherList = new ArrayList<>();

        while (true) {
            Object weather = queue.poll();

            if (weather == null) {
                break;
            }

            weatherList.add(weather);
        }

        if (weatherList.isEmpty()) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {
                PrintStream printStream = new PrintStream(out, false, "UTF-8");
                CSVPrinter printer = new CSVPrinter(printStream, CSVFormat.RFC4180);
                printer.printRecord(
                        "id|string",
                        "x|double",
                        "y|double",
                        "city|string",
                        "cityCode|long",
                        "TIMESTAMP|long",
                        "high|int",
                        "low|int",
                        "temperature|int",
                        "hasRain|int");
                int count = 0;

                for (Object weather : weatherList) {
                    ReadContext rc = JsonPath.parse(weather, Configuration.defaultConfiguration().addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL));

                    try {
                        printer.printRecord(
                                UUID.randomUUID().toString(),
                                rc.read("$.coord.lon", Double.class),
                                rc.read("$.coord.lat", Double.class),
                                rc.read("$.name", String.class),
                                rc.read("$.id", Integer.class),
                                rc.read("$.dt", Long.class) * 1000,
                                rc.read("$.main.temp_max", Double.class).intValue(),
                                rc.read("$.main.temp_min", Double.class).intValue(),
                                rc.read("$.main.temp", Double.class).intValue(),
                                rc.read("$.rain") == null || rc.read("$.rain.3h") == null ? 0 : (rc.read("$.rain.3h", Double.class) > 0 ? 1 : 0));
                        count++;
                    } catch (Exception ex) {
                        getLogger().error("Bad weather record", ex);
                    }
                }

                printer.flush();
                printStream.flush();
                out.flush();
                getLogger().info("Got {} weather record(s).", new Object[] { count });
            }
        });

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), "text/csv");
        attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".csv");
        flowFile = session.putAllAttributes(flowFile, attributes);
        session.getProvenanceReporter().receive(flowFile, "http://api.openweathermap.org/data/2.5/weather");
        session.transfer(flowFile, REL_SUCCESS);
    }
}
