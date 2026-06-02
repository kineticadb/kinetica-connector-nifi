# Kinetica NiFi Connector Distribution

This directory contains pre-built NAR files for the Kinetica NiFi Connector.

## Installation

1. Copy the NAR file to your NiFi `lib/` directory:
   ```bash
   cp nifi-GPUdbNiFi-nar-*.nar $NIFI_HOME/lib/
   ```

2. Restart NiFi to load the new processors.

## Building from Source

To build the NAR file from source:

```bash
# Set Java 21 (required for NiFi 2.x)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Build the NAR package (automatically copies to dist/)
mvn clean package -DskipTests

# The NAR file will be automatically copied to this directory
```

**Note**: The Maven build is configured to automatically copy the NAR file to this
`dist/` directory during the `package` phase.

## Version Information

- **Connector Version**: 7.2
- **NiFi Version**: 2.7.0
- **Java Version**: 21
- **Kinetica API Version**: 7.2.3

## Included Processors

### Put Processors (Ingestion)
- **PutKinetica** - Insert data from FlowFile attributes
- **PutKineticaFromFile** - Insert data from CSV/TSV files
- **PutKineticaFromJSON** - Insert data from JSON files (array or NDJSON)
- **PutKineticaFromAvro** - Insert data from Avro binary files
- **PutKineticaRecord** - Insert data using NiFi Record API (supports any format)

### Query Processors (SQL Queries)
- **QueryKineticaToCSV** - Execute SQL query and output as CSV
- **QueryKineticaToJSON** - Execute SQL query and output as JSON
- **QueryKineticaToAvro** - Execute SQL query and output as Avro

### Get Processors (Table Monitoring)
- **GetKineticaToCSV** - Monitor table changes and output as CSV
- **GetKineticaToJSON** - Monitor table changes and output as JSON
- **GetKineticaToAvro** - Monitor table changes and output as Avro

### Utility Processors
- **ExecuteKineticaSQL** - Execute arbitrary SQL statements (DDL/DML)
- **ListKineticaTables** - List tables in a Kinetica schema
