# Kinetica NiFi Connector

This project is aimed to make Kinetica both a data source and data sink for NiFi.

The documentation can be found at http://www.kinetica.com/docs/7.2/index.html.

For changes to the connector API, please refer to CHANGELOG.md.  For changes
to Kinetica functions, please refer to CHANGELOG-FUNCTIONS.md.

## Version 7.2.0.0

This version has been modernized for **NiFi 2.x** and **Java 21**.

### Requirements

- **Java 21** or later (required for NiFi 2.x)
- **Apache NiFi 2.0.0** or later
- **Kinetica 7.2.x** server

### Key Changes from v7.1

- Upgraded from NiFi 1.3.0 to NiFi 2.0.0
- Upgraded from Java 7 to Java 21
- New package structure: `com.kinetica.nifi.processors` (previously `com.gisfederal.gpudb.processors.GPUdbNiFi`)
- Refactored with 3-tier inheritance hierarchy for maintainability
- Fixed CSV parser performance issue (10-100x improvement for large files)
- Fixed ZeroMQ connection leak in Get processors
- Added null-safe utility methods
- Added table name validation to prevent SQL injection
- **NEW: SSL/TLS support** for secure connections
- **NEW: Connection pooling** for improved performance
- **NEW: Configurable timeouts** for connection and socket operations
- **NEW: Enhanced Expression Language support** for dynamic configuration

-----

## NiFi Connector Developer Manual

The following guide provides step by step instructions to get started using
*Kinetica* as a data source to read from and write to.  Source code for the
connector can be found at:

* https://github.com/kineticadb/kinetica-connector-nifi

### Building the Kinetica NiFi Connector

The connector jar can be built with *Maven*.

1. Download the connector source:

```bash
git clone https://github.com/kineticadb/kinetica-connector-nifi.git
cd kinetica-connector-nifi
```

2. Set Java 21 and build the connector (skipping tests):

```bash
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
mvn clean package -DskipTests
```

In order to run the tests as part of the build process, a *Kinetica* instance
must be available:

```bash
mvn clean package -Dkinetica.url=http://<host>:<port> -Dkinetica.username=<user> -Dkinetica.password=<pass>
```

The NAR file will be created at `nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.0.0.nar`

### Installing the Kinetica NiFi Connector into NiFi

Copy the NAR file to your NiFi installation's `lib/` directory and restart NiFi:

```bash
cp nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.0.0.nar $NIFI_HOME/lib/
```

-----

## Processors

### Getting Streaming Data from Kinetica to JSON or CSV Files

#### GetKineticaToCSV

Monitors a Kinetica table and outputs new records as CSV files.

1. Drag a new *Processor* onto the flow and select the *GetKineticaToCSV* type

2. Configure the *Properties* tab:

| Property | Required | Description |
|----------|----------|-------------|
| Server URL | Yes | Kinetica server URL (e.g., `http://172.10.20.30:9191`) |
| Table Name | Yes | The name of the table to read from |
| Table Monitor URL | Yes | ZeroMQ endpoint for table monitoring (e.g., `tcp://172.10.20.30:9002`) |
| Delimiter | No | CSV delimiter (default: tab) |
| Username | No | Kinetica login username; required if authentication is enabled |
| Password | No | Kinetica login password; required if authentication is enabled |

The output is a CSV file containing the record inserted into the *Kinetica* table.

#### GetKineticaToJSON

Monitors a Kinetica table and outputs new records as JSON.

1. Drag a new *Processor* onto the flow and select the *GetKineticaToJSON* type

2. Configure the *Properties* tab:

| Property | Required | Description |
|----------|----------|-------------|
| Server URL | Yes | Kinetica server URL (e.g., `http://172.10.20.30:9191`) |
| Table Name | Yes | The name of the table to read from |
| Table Monitor URL | Yes | ZeroMQ endpoint for table monitoring (e.g., `tcp://172.10.20.30:9002`) |
| Username | No | Kinetica login username; required if authentication is enabled |
| Password | No | Kinetica login password; required if authentication is enabled |

The output is a JSON file containing the record inserted into the *Kinetica* table.

### Saving Data to Kinetica Using NiFi Attributes

#### PutKinetica

Bulk loads FlowFile attributes to Kinetica in batch intervals.

1. Drag a new *Processor* onto the flow and select the *PutKinetica* type

2. In the *Settings* tab, under *Auto terminate Relationships*, check the *failure* and *success* options.

3. Configure the *Properties* tab:

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Server URL | Yes | | Kinetica server URL (e.g., `http://172.10.20.30:9191`) |
| Table Name | Yes | | The name of the table to write to |
| Schema | No | | Schema definition (see Schema Format below) |
| Collection Name | No | | Set this value to create the table in a collection |
| Batch Size | No | 10000 | Records per batch |
| Username | No | | Kinetica login username; required if authentication is enabled |
| Password | No | | Kinetica login password; required if authentication is enabled |
| Update on Existing PK | No | false | If a PK matches, update the existing record (true) or discard new record (false) |
| Replicate Table | No | false | Create as replicated table (true) or distributed table (false) |
| Date Format | No | | Date parsing format (e.g., `dd-MM-yyyy hh:mm:ss`) |
| TimeZone | No | | Timezone if date is not from local timezone |
| Use SSL/TLS | No | false | Enable SSL/TLS for secure connections |
| Bypass SSL Certificate Check | No | false | Skip SSL cert verification (dev only) |
| Connection Timeout | No | 30 sec | Connection timeout |
| Socket Timeout | No | 60 sec | Socket timeout |
| Connection Pool Size | No | 4 | Max connections in pool |

4. Specifying data to be saved into *Kinetica*:

   * Place processors upstream which assign values to user-defined attributes named `<field name>`, where `<field name>` is the name of a field in your table
   * Each record written to your table will contain field values of:
     - the value in the attributes with names `<field name>` or
     - the value of *null* if no attribute is found with that field name

### Saving Data to Kinetica Using Delimited Files

#### PutKineticaFromFile

Bulk loads delimited file contents (CSV, TSV) to Kinetica.

1. Drag a new *Processor* onto the flow and select the *PutKineticaFromFile* type

2. In the *Settings* tab, under *Auto terminate Relationships*, check the *failure* and *success* options.

3. Configure the *Properties* tab:

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Server URL | Yes | | Kinetica server URL (e.g., `http://172.10.20.30:9191`) |
| Table Name | Yes | | The name of the table to write to |
| Schema | No | | Schema definition (see Schema Format below) |
| Collection Name | No | | Set this value to create the table in a collection |
| Delimiter | Yes | `,` | Field delimiter character (e.g., comma, tab, pipe) |
| Quote Character | No | `"` | Character used to quote column data |
| Escape Character | No | `"` | Character used to escape other characters |
| File Has Header | No | true | Whether the first line is a header row |
| Skip Errors | Yes | true | Continue on parse errors (true) or stop on first error (false) |
| Batch Size | No | 10000 | Records per batch |
| Username | No | | Kinetica login username; required if authentication is enabled |
| Password | No | | Kinetica login password; required if authentication is enabled |
| Update on Existing PK | No | false | If a PK matches, update the existing record (true) or discard new record (false) |
| Replicate Table | No | false | Create as replicated table (true) or distributed table (false) |
| Date Format | No | | Date parsing format (e.g., `dd-MM-yyyy hh:mm:ss`) |
| TimeZone | No | | Timezone if date is not from local timezone |

4. Create a connector between the data source processor and the *PutKineticaFromFile* processor

**Performance Note:** For large files, chunk into ~1M rows to avoid memory issues.

-----

## Schema Format

The schema is a comma-separated list of column definitions:
```
column_name|type|annotation1|annotation2...
```

**Types:** `String`, `Int`, `Long`, `Float`, `Double`

**Annotations:** `data`, `store_only`, `text_search`, `primary_key`, `timestamp`, etc.

**Example:**
```
id|Long|data|primary_key,x|Float|data,y|Float|data,name|String|data,timestamp|Long|data|timestamp
```

Another example:
```
X|Float|data,Y|Float|data,TIMESTAMP|Long|data,TEXT|String|store_only|text_search
```

For more details on schemas, read the *Kinetica* documentation.

-----

## SSL/TLS Configuration

For secure connections to Kinetica:

```
Server URL: https://kinetica.example.com:8082
Use SSL/TLS: true
Bypass SSL Certificate Check: false  (set to true only for self-signed certs in dev)
```

## Connection Tuning

For high-throughput scenarios:

| Setting | Recommended | Notes |
|---------|-------------|-------|
| Batch Size | 10000-50000 | Higher = better throughput, more memory |
| Connection Pool Size | 4-8 | Match to concurrent tasks |
| Connection Timeout | 30 sec | Increase for slow networks |
| Socket Timeout | 60 sec | Increase for large batches |

-----

## Architecture

```
AbstractKineticaProcessor (base)
├── SSL/TLS support
├── Connection pooling
├── Timeout configuration
├── AbstractPutKineticaProcessor (Put operations)
│   ├── PutKinetica
│   └── PutKineticaFromFile
└── AbstractGetKineticaProcessor (Get operations)
    ├── GetKineticaToCSV
    └── GetKineticaToJSON
```

-----

## Source Code

- https://github.com/kineticadb/kinetica-connector-nifi

## License

Copyright (c) Kinetica DB Inc.
