# Kinetica NiFi Connector User Guide

**Version:** 7.2
**Compatibility:** Apache NiFi 2.7.0+, Java 21+, Kinetica 7.2+

## Table of Contents

1. [Overview](#overview)
2. [Requirements](#requirements)
3. [Installation](#installation)
4. [Building from Source](#building-from-source)
5. [Configuration](#configuration)
6. [Processors](#processors)
   - [PutKinetica](#putkinetica)
   - [PutKineticaFromFile](#putkineticafromfile)
   - [PutKineticaFromJSON](#putkineticafromjson)
   - [PutKineticaFromAvro](#putkineticafromavro)
   - [GetKineticaToCSV](#getkineticatocsv)
   - [GetKineticaToJSON](#getkineticatojson)
   - [QueryKineticaToCSV](#querykineticatocsv)
   - [QueryKineticaToJSON](#querykineticatojson)
   - [QueryKineticaToAvro](#querykineticatoavro)
   - [ListKineticaTables](#listkineticatables)
7. [Schema Definition Format](#schema-definition-format)
8. [Example Dataflows](#example-dataflows)
9. [Performance Tuning](#performance-tuning)
10. [Troubleshooting](#troubleshooting)

---

## Overview

The Kinetica NiFi Connector provides a set of Apache NiFi processors for integrating with Kinetica, a high-performance analytics database. The connector enables:

- **Data Ingestion**: Load data from various sources (CSV, JSON, Avro, FlowFile attributes) into Kinetica tables
- **Real-time Streaming**: Monitor Kinetica tables for changes and stream new records
- **SQL Queries**: Execute SELECT queries and export results to CSV, JSON, or Avro
- **Table Discovery**: List and discover tables with metadata
- **Data Lake Integration**: Export to Avro format for Spark, Hive, and other big data systems

### Key Features

- Full NiFi 2.7.0 compatibility with Java 21
- SSL/TLS support for secure connections
- Connection pooling for high throughput
- Streaming mode for large result sets (100K+ records)
- Automatic table creation from schema definitions or Avro schemas
- Batch processing with configurable sizes
- Retry logic with exponential backoff
- Comprehensive error handling
- Avro format support for data lake integration
- Full support for DECIMAL and ARRAY column types
- Configurable cluster discovery and failover behavior

---

## Requirements

| Component | Version |
|-----------|---------|
| Apache NiFi | 2.7.0 or later |
| Java | 21 or later |
| Kinetica | 7.2 or later |
| Maven | 3.8+ (for building) |

---

## Installation

### Option 1: Pre-built NAR File

1. Get the NAR file from the `dist/` directory: `nifi-GPUdbNiFi-nar-7.2.x.y.nar`
2. Copy to NiFi's extensions directory:
   ```bash
   cp dist/nifi-GPUdbNiFi-nar-7.2.x.y.nar $NIFI_HOME/extensions/
   ```
3. Restart NiFi:
   ```bash
   $NIFI_HOME/bin/nifi.sh restart
   ```

### Option 2: Build from Source

See [Building from Source](#building-from-source) below. The NAR file is automatically
copied to the `dist/` directory during the build.

---

## Building from Source

### Prerequisites

- Java 21 JDK
- Maven 3.8+
- Git

### Build Steps

```bash
# Clone the repository
git clone https://github.com/kineticadb/kinetica-connector-nifi.git
cd kinetica-connector-nifi

# Set Java 21
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Build the project (NAR is automatically copied to dist/)
mvn clean package -DskipTests

# The NAR file will be in:
# dist/nifi-GPUdbNiFi-nar-7.2.x.y.nar
# (also available at nifi-GPUdbNiFi-nar/target/)
```

### Running Tests

```bash
# Unit tests only
mvn test

# Integration tests (requires running Kinetica)
mvn verify -pl integration-tests -Dkinetica.url=http://localhost:9191

# With authentication
mvn verify -pl integration-tests \
  -Dkinetica.url=http://localhost:9191 \
  -Dkinetica.username=admin \
  -Dkinetica.password=yourpassword
```

### Build Profiles

```bash
# Run code quality checks (PMD, SpotBugs)
mvn verify -Pquality

# Run security vulnerability scan (OWASP)
mvn verify -Psecurity
```

---

## Configuration

### Common Properties

All Kinetica processors share these connection properties:

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Server URL | Yes | - | Kinetica server URL (e.g., `http://localhost:9191`) |
| Table Name | Yes* | - | Target table name (supports `schema.table_name` format) |
| Username | No | - | Authentication username |
| Password | No | - | Authentication password (sensitive) |
| Use SSL/TLS | No | false | Enable secure connections |
| Bypass SSL Certificate Check | No | false | Skip certificate validation (dev only) |
| Connection Timeout | No | 30 sec | Maximum connection wait time |
| Socket Timeout | No | 60 sec | Maximum socket wait time |
| Connection Pool Size | No | 4 | Number of pooled connections |
| Disable Auto Discovery | No | false | Disable automatic cluster node discovery |
| Disable Failover | No | false | Disable automatic failover to other nodes |

*Not required for ListKineticaTables processor

### Kinetica Head Node Configuration

When connecting through a Kinetica head node, you may need to disable auto-discovery
and failover to prevent the client from bypassing the head node:

```
Disable Auto Discovery: true
Disable Failover: true
```

This ensures all connections go through your Kinetica head node endpoint.

### Expression Language Support

Most properties support NiFi Expression Language for dynamic configuration:

```
Server URL: ${kinetica.server}
Table Name: ${kinetica.table.prefix}_${now():format('yyyyMMdd')}
```

---

## Processors

### PutKinetica

**Purpose**: Bulk loads FlowFile attributes to Kinetica.

Each FlowFile's attributes are mapped to table columns by name.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Batch Size | Yes | 500 | Records per batch |
| Schema Definition | No | - | Table schema in pipe-delimited format |
| Avro Schema | No | - | Table schema as Avro JSON (alternative to Schema Definition) |
| Collection Name | No | - | Optional collection/schema |
| Update on Existing PK | No | false | Update records with matching primary key |
| Replicate Table | No | false | Create replicated table |
| Date Format | No | - | Pattern for parsing dates (e.g., `yyyy/MM/dd HH:mm:ss`) |
| Timezone | No | System | Timezone for date parsing (e.g., `UTC`, `EST`) |

**Note**: If the table doesn't exist, either `Schema Definition` or `Avro Schema` must be
provided. `Schema Definition` takes precedence if both are set.

#### Relationships

- **success**: FlowFiles successfully written
- **failure**: FlowFiles that failed

#### Example Usage

For a table with columns `(id, name, value)`:

```
FlowFile Attributes:
  id = "123"
  name = "Product A"
  value = "99.95"
```

---

### PutKineticaFromFile

**Purpose**: Bulk loads delimited file contents (CSV, TSV) to Kinetica.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Delimiter | Yes | `,` | Field delimiter (`,`, `\t`, `|`) |
| Escape Character | No | `"` | Escape character |
| Quote Character | No | `"` | Quote character for fields |
| File Has Header | No | true | Skip first row as header |
| Skip Errors | No | true | Continue on invalid records |
| Batch Size | Yes | 500 | Records per batch |

#### Relationships

- **success**: Files successfully loaded
- **failure**: Files that failed or bad records (if Skip Errors=true)

#### Input Format

CSV file with columns matching the table schema:

```csv
id,name,value,timestamp
1,Product A,99.95,1703001600000
2,Product B,149.99,1703001700000
```

#### Performance Note

For large files (>1M rows), chunk into smaller files for optimal throughput.

---

### PutKineticaFromJSON

**Purpose**: Bulk loads JSON file contents to Kinetica.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| JSON Format | Yes | ARRAY | `ARRAY` or `NDJSON` (newline-delimited) |
| Skip Errors | No | true | Continue on invalid records |
| Batch Size | Yes | 500 | Records per batch |

#### Input Formats

**JSON Array** (`ARRAY`):
```json
[
  {"id": 1, "name": "Product A", "value": 99.95},
  {"id": 2, "name": "Product B", "value": 149.99}
]
```

**Newline-Delimited JSON** (`NDJSON`):
```json
{"id": 1, "name": "Product A", "value": 99.95}
{"id": 2, "name": "Product B", "value": 149.99}
```

NDJSON is recommended for large files as it supports streaming parsing.

---

### PutKineticaFromAvro

**Purpose**: Bulk loads Avro file contents to Kinetica.

Reads Avro container format files with embedded schema and inserts records into a Kinetica table.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Skip Errors | No | true | Continue on invalid records |
| Batch Size | Yes | 500 | Records per batch |
| Date Format | No | - | Pattern for parsing dates |
| Timezone | No | System | Timezone for date parsing |

#### Relationships

- **success**: Files successfully loaded
- **failure**: Files that failed

#### Input Format

Avro container format with embedded schema. The schema field names should match the Kinetica table column names.

Example Avro schema:
```json
{
  "type": "record",
  "name": "ProductRecord",
  "namespace": "com.kinetica.example",
  "fields": [
    {"name": "id", "type": "long"},
    {"name": "name", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "quantity", "type": "int"}
  ]
}
```

#### Type Mapping

| Avro Type | Kinetica Type |
|-----------|---------------|
| `long` | Long |
| `int` | Integer |
| `double` | Double |
| `float` | Float |
| `string` | String |
| `bytes` | Bytes |
| `boolean` | Integer (0/1) |
| `array` | Array (JSON string) |
| Numeric to DECIMAL | DECIMAL (formatted string) |

#### DECIMAL and ARRAY Support

**DECIMAL columns**: Numeric values are automatically converted to properly scaled decimal strings based on the column's precision and scale.

**ARRAY columns**: Avro arrays are converted to JSON array strings for storage in Kinetica's ARRAY columns.

---

### GetKineticaToCSV

**Purpose**: Monitors a Kinetica table for new records and outputs them as CSV.

Uses ZeroMQ table monitors for real-time notifications.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Table Monitor URL | Yes | - | ZeroMQ endpoint (e.g., `tcp://localhost:9002`) |
| Delimiter | No | `\t` | Output delimiter |

#### Relationships

- **success**: FlowFiles containing new records as CSV

#### Output Format

```csv
id|Long|primary_key	name|String|data	value|Double|data
1	Product A	99.95
2	Product B	149.99
```

Header includes column type information in Kinetica schema format.

---

### GetKineticaToJSON

**Purpose**: Monitors a Kinetica table for new records and outputs them as JSON.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Table Monitor URL | Yes | - | ZeroMQ endpoint |

#### Output Format

```json
[
  {"id": 1, "name": "Product A", "value": 99.95},
  {"id": 2, "name": "Product B", "value": 149.99}
]
```

---

### QueryKineticaToCSV

**Purpose**: Executes SQL SELECT queries and outputs results as CSV.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| SQL Query | Yes | - | SELECT query (supports Expression Language) |
| Page Size | No | 10000 | Records per page |
| Maximum Records | No | -1 | Limit results (-1 = unlimited) |
| Use Streaming Mode | No | false | Use server-side paging tables |
| Paging Table TTL | No | 300 | TTL for paging tables (seconds) |
| Delimiter | Yes | `,` | Output delimiter |
| Include Header | No | true | Include column names |
| Quote Character | No | `"` | Quote character |

#### Streaming Mode

Enable for queries returning >100K records. Uses Kinetica's server-side paging tables:
- Avoids re-executing query for each page
- Better memory efficiency
- Automatic cleanup of temporary tables

#### Example

```sql
SELECT id, name, value
FROM products
WHERE category = '${category}'
ORDER BY value DESC
```

---

### QueryKineticaToJSON

**Purpose**: Executes SQL SELECT queries and outputs results as JSON.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| SQL Query | Yes | - | SELECT query |
| Page Size | No | 10000 | Records per page |
| Maximum Records | No | -1 | Limit results |
| Use Streaming Mode | No | false | Use server-side paging |
| JSON Format | Yes | ARRAY | `ARRAY` or `NDJSON` |
| Pretty Print | No | false | Format with indentation |

---

### QueryKineticaToAvro

**Purpose**: Executes SQL SELECT queries and outputs results as Avro.

Generates Avro container format files with embedded schema, suitable for downstream Avro-compatible systems like Kafka, Spark, or HDFS.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| SQL Query | Yes | - | SELECT query (supports Expression Language) |
| Include Schema | No | true | Include schema in Avro container (false = raw binary) |
| Avro Namespace | No | com.kinetica | Namespace for generated Avro schema |
| Page Size | No | 10000 | Records per page |
| Maximum Records | No | -1 | Limit results (-1 = unlimited) |
| Use Streaming Mode | No | false | Use server-side paging tables |
| Paging Table TTL | No | 300 | TTL for paging tables (seconds) |

#### Relationships

- **success**: FlowFiles containing query results as Avro
- **failure**: Failed queries

#### Output Attributes

| Attribute | Description |
|-----------|-------------|
| mime.type | `application/avro-binary` |
| record.count | Number of records in output |
| avro.schema | JSON representation of Avro schema |

#### Type Mapping

| Kinetica Type | Avro Type |
|---------------|-----------|
| Long | `long` |
| Integer | `int` |
| Double | `double` |
| Float | `float` |
| String | `string` |
| Bytes | `bytes` |
| DECIMAL | `string` (formatted) |
| ARRAY | `array` (typed elements) |

#### DECIMAL and ARRAY Support

**DECIMAL columns**: Values are exported as properly formatted decimal strings preserving precision and scale.

**ARRAY columns**: Kinetica's JSON array strings are parsed and converted to typed Avro arrays. The array element type is determined from the column metadata.

#### Example

```sql
SELECT id, name, price, tags
FROM products
WHERE category = 'electronics'
ORDER BY price DESC
```

Output: Avro container file with schema derived from query result columns.

---

### ListKineticaTables

**Purpose**: Lists tables in Kinetica and emits a FlowFile for each table.

Useful for dynamic table discovery and metadata-driven workflows.

#### Properties

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Table Name Pattern | Yes | `*` | Pattern to match (`*` = all, `schema.*` = schema) |
| Include Table Sizes | No | true | Include row counts |
| Include Child Tables | No | true | Include views, projections |
| Table Type Filter | No | - | Filter by type: `TABLE`, `VIEW`, `COLLECTION`, etc. |

#### Output Attributes

Each FlowFile includes:

| Attribute | Description |
|-----------|-------------|
| kinetica.table.name | Fully qualified table name |
| kinetica.table.schema | Schema/collection name |
| kinetica.table.type | TABLE, VIEW, COLLECTION, etc. |
| kinetica.table.row_count | Number of rows |
| kinetica.table.type_id | Kinetica type ID |
| kinetica.table.type_schema | Avro schema definition |

---

## Schema Definition Format

When creating tables automatically, use this format:

```
column1|type|annotation1|annotation2,column2|type|annotation,...
```

### Supported Types

| Type | Java Class | Notes |
|------|------------|-------|
| `String` | String | Variable-length text |
| `Long` | Long | 64-bit integer |
| `Integer` / `Int` | Integer | 32-bit integer |
| `Double` | Double | 64-bit floating point |
| `Float` | Float | 32-bit floating point |
| `Bytes` | ByteBuffer | Binary data |

### Extended Types (via Annotations)

| Extended Type | Base Type | Annotations | Description |
|---------------|-----------|-------------|-------------|
| DECIMAL | String | `decimal(precision,scale)` | Fixed-point decimal numbers |
| ARRAY | String | `array(element_type)` | Arrays stored as JSON |
| TIMESTAMP | Long | `timestamp` | Milliseconds since epoch |
| DATE | String | `date` | Date in YYYY-MM-DD format |
| TIME | String | `time` | Time in HH:MM:SS format |
| DATETIME | String | `datetime` | Combined date and time |
| IPV4 | String | `ipv4` | IPv4 address |
| UUID | String | `uuid` | UUID string |
| WKT | String | `wkt` | Well-Known Text geometry |

### Common Annotations

| Annotation | Description |
|------------|-------------|
| `data` | Standard data column |
| `primary_key` | Primary key column |
| `timestamp` | Timestamp column |
| `store_only` | Store but don't index |
| `text_search` | Enable text search |
| `nullable` | Allow null values |

### Examples

**Simple table:**
```
id|Long|primary_key,name|String|data,value|Double|data
```

**With timestamps:**
```
id|Long|primary_key,event_time|Long|timestamp,message|String|data
```

**With text search:**
```
id|Long|primary_key,title|String|data|text_search,content|String|store_only|text_search
```

---

## Avro Schema for Table Creation

As an alternative to the pipe-delimited Schema Definition, you can use Avro JSON schemas
to define table structure. This is useful when you already have Avro schemas from upstream
systems like Kafka or data lakes.

### Avro Schema Example

```json
{
  "type": "record",
  "name": "SensorData",
  "fields": [
    {"name": "sensor_id", "type": "string"},
    {"name": "temperature", "type": "double"},
    {"name": "timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "location", "type": ["null", "string"]}
  ]
}
```

### Avro to Kinetica Type Mapping

| Avro Type | Logical Type | Kinetica Type | Column Property |
|-----------|--------------|---------------|-----------------|
| `int` | - | Integer | data |
| `long` | - | Long | data |
| `float` | - | Float | data |
| `double` | - | Double | data |
| `boolean` | - | Integer | data, int8 |
| `string` | - | String | data |
| `bytes` | - | ByteBuffer | data |
| `long` | `timestamp-millis` | Long | data, timestamp |
| `long` | `timestamp-micros` | Long | data, timestamp |
| `int` | `date` | String | data, date |
| `int` | `time-millis` | String | data, time |
| `bytes` | `decimal` | String | data, decimal |

### Nullable Fields

Avro union types containing `null` are automatically marked as nullable in Kinetica:

```json
{"name": "optional_field", "type": ["null", "string"]}
```

This creates a nullable String column in Kinetica.

### Complex Types

Complex Avro types (arrays, maps, nested records) are stored as JSON strings in Kinetica:

```json
{"name": "tags", "type": {"type": "array", "items": "string"}}
```

---

## Example Dataflows

### 1. CSV File Ingestion

```
[GetFile] -> [PutKineticaFromFile] -> [LogAttribute]
                    |
                    +--[failure]--> [PutFile (bad records)]
```

Configuration:
- GetFile: Input directory with CSV files
- PutKineticaFromFile: Table name, delimiter, batch size

### 2. Real-time Table Monitoring

```
[GetKineticaToJSON] -> [SplitJson] -> [EvaluateJsonPath] -> [PutFile]
```

Configuration:
- GetKineticaToJSON: Table name, monitor URL
- SplitJson: Split array into individual records

### 3. Scheduled Data Export

```
[GenerateFlowFile] -> [QueryKineticaToCSV] -> [PutS3Object]
     (cron)
```

Configuration:
- GenerateFlowFile: CRON schedule
- QueryKineticaToCSV: SQL query with date range

### 4. Dynamic Table Processing

```
[ListKineticaTables] -> [QueryKineticaToJSON] -> [PutFile]
                              ^
                              |
                        (uses ${kinetica.table.name})
```

### 5. Avro File Ingestion

```
[GetFile] -> [PutKineticaFromAvro] -> [LogAttribute]
                    |
                    +--[failure]--> [PutFile (bad records)]
```

Configuration:
- GetFile: Input directory with Avro files
- PutKineticaFromAvro: Table name, batch size

### 6. Avro Export to Data Lake

```
[GenerateFlowFile] -> [QueryKineticaToAvro] -> [PutHDFS / PutS3Object]
     (cron)
```

Configuration:
- QueryKineticaToAvro: SQL query, Avro namespace, streaming mode for large exports
- Output: Avro container files compatible with Spark, Hive, Presto

---

## Performance Tuning

### Batch Size

| Use Case | Recommended Batch Size |
|----------|------------------------|
| Small records (<100 bytes) | 5,000 - 10,000 |
| Medium records (100-1000 bytes) | 1,000 - 5,000 |
| Large records (>1000 bytes) | 500 - 1,000 |

### Concurrent Tasks

- **PutKinetica processors**: 2-4 concurrent tasks
- **Query processors**: 1-2 concurrent tasks
- **Get processors**: 1 task (ZeroMQ single subscriber)

### Large File Handling

For files >1 million rows:
1. Chunk files before processing
2. Use `PutKineticaFromJSON` with NDJSON format
3. Enable streaming mode for queries

### Connection Pool Size

- Match to NiFi concurrent tasks
- Default of 4 works for most cases
- Increase for high-throughput scenarios

### Streaming Mode

Enable for:
- Queries returning >100,000 records
- Long-running exports
- Memory-constrained environments

---

## Troubleshooting

### Connection Issues

**Error**: `Failed to connect to Kinetica`

- Verify Server URL is correct
- Check network connectivity
- Confirm Kinetica is running
- Verify credentials if authentication is enabled

### SSL/TLS Issues

**Error**: `SSL handshake failed`

- Ensure `Use SSL/TLS` is enabled for HTTPS URLs
- For self-signed certificates, enable `Bypass SSL Certificate Check` (dev only)
- Verify certificate chain is valid

### Table Not Found

**Error**: `Table does not exist and no schema provided`

- Provide a Schema Definition to auto-create the table
- Or create the table manually in Kinetica first

### Type Conversion Errors

**Error**: `Invalid data type for column`

- Check that FlowFile attribute values match expected column types
- Use Date Format property for timestamp parsing
- Enable Skip Errors to continue processing on bad records

### Memory Issues

**Symptom**: OutOfMemoryError during large queries

- Enable Streaming Mode
- Reduce Page Size
- Set Maximum Records limit
- Increase NiFi heap size

### ZeroMQ Connection Leaks

**Symptom**: Too many open connections

- Ensure processors are properly stopped before reconfiguration
- Check NiFi logs for cleanup errors
- Restart NiFi if connections accumulate

### Debugging

Enable debug logging in NiFi's `logback.xml`:

```xml
<logger name="com.kinetica.nifi" level="DEBUG"/>
```

---

## Support

- **Issues**: https://github.com/kineticadb/kinetica-connector-nifi/issues
- **Kinetica Support**: https://support.kinetica.com/

---

## License

Apache License 2.0
