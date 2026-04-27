# Kinetica NiFi Connector

Apache NiFi processors for high-speed data ingestion and retrieval with [Kinetica](https://www.kinetica.com/).

Uses the **native GPUdb Java API** with `BulkInserter` for multi-head parallel ingest — significantly faster than standard JDBC for large data volumes. Query-based processors use GPUdb `getRecords()` and `executeSql()` REST APIs for efficient data retrieval with offset/limit batching.

## Compatibility

| Component | Version |
|-----------|---------|
| Apache NiFi | **2.7.x+** |
| Java | **21** |
| Kinetica | **7.2.x** |
| GPUdb Java API | **7.2.3.17** |

## Processors (10 total)

### Put Processors (Ingest)

| Processor | Input Format | Description |
|-----------|-------------|-------------|
| **PutKinetica** | FlowFile attributes | Inserts records from FlowFile attributes via BulkInserter |
| **PutKineticaFromCSV** | CSV/delimited | Parses CSV FlowFile content and inserts rows via BulkInserter |
| **PutKineticaFromJSON** | JSON | Parses JSON array or object and inserts via BulkInserter |
| **PutKineticaFromAvro** | Avro | Reads Avro container format and inserts via BulkInserter |

### Query Processors (Retrieve — recommended)

| Processor | Output Format | Description |
|-----------|--------------|-------------|
| **QueryKineticaToCSV** | CSV | Queries table or executes SQL, outputs CSV with batching |
| **QueryKineticaToJSON** | JSON | Queries table or executes SQL, outputs JSON array with batching |
| **QueryKineticaToAvro** | Avro | Queries table or executes SQL, outputs Avro container with batching |

### ZMQ Monitor Processors (Legacy — requires ZeroMQ ports)

| Processor | Output Format | Description |
|-----------|--------------|-------------|
| **GetKineticaToCSV** | CSV | Monitors table via ZeroMQ, outputs new records as CSV |
| **GetKineticaToJSON** | JSON | Monitors table via ZeroMQ, outputs new records as JSON |
| **GetKineticaToAvro** | Avro | Monitors table via ZeroMQ, outputs new records as Avro |

## Quick Start

### Build

```bash
# Requires Java 21
JAVA_HOME=/path/to/java-21 mvn clean package
```

The NAR file is produced at:
```
nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.3.0.nar
```

### Deploy to NiFi

**Standalone NiFi:**
```bash
cp nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.3.0.nar $NIFI_HOME/extensions/
$NIFI_HOME/bin/nifi.sh restart
```

**Docker NiFi** (use the autoload directory — it persists across restarts):
```bash
docker cp nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.3.0.nar <container>:/opt/nifi/nifi-current/nar_extensions/
docker restart <container>
```

> **Important:** In Docker, `/opt/nifi/nifi-current/extensions/` is ephemeral.
> Always use `nar_extensions/` (the `nifi.nar.library.autoload.directory`).

The processors will appear under the `com.gisfederal.gpudb.processors.GPUdbNiFi` group.

## Processor Configuration

### Expression Language & Parameter Context

All non-sensitive, non-boolean properties support **Expression Language** (`${ENV_VAR}`) with `FLOWFILE_ATTRIBUTES` scope and **NiFi Parameter Context** (`#{param}`).

- `#{param}` works on ALL properties (including Password — resolved by NiFi before EL)
- `${attr}` / `${ENV_VAR}` works on properties with FLOWFILE_ATTRIBUTES scope
- Password: No EL (sensitive), but `#{param}` works
- Boolean flags: No EL (validator incompatible), but `#{param}` works

### Common Connection Properties (all processors)

| Property | Required | Default | EL | Description |
|----------|----------|---------|-----|-------------|
| Disable Auto Discovery | No | `false` | ❌ | Disable automatic cluster node discovery. Set to `true` when connecting through a proxy or load balancer where internal cluster IPs are not reachable |
| Disable Failover | No | `false` | ❌ | Disable automatic failover to other cluster nodes. Set to `true` when using a single-endpoint proxy |

### PutKinetica

| Property | Required | Default | EL | Description |
|----------|----------|---------|-----|-------------|
| Server URL | Yes | — | ✅ | Kinetica server URL (e.g. `http://host:9191`) |
| Table Name | Yes | — | ✅ | Target table name |
| Schema | No | — | ✅ | Table schema definition (`col\|Type\|subtype,...`) |
| Avro Schema | No | — | ✅ | Avro JSON schema for auto table creation (see below) |
| Batch Size | No | `100` | ✅ | Records per batch |
| Username | No | — | ✅ | Kinetica username |
| Password | No | — | 🔒 | Kinetica password (sensitive) |
| Update on Existing PK | No | `false` | ❌ | Update existing primary key records |
| Replicate Table | No | `false` | ❌ | Create replicated table |

### PutKineticaFromCSV (CSV)

| Property | Required | Default | EL | Description |
|----------|----------|---------|-----|-------------|
| Server URL | Yes | — | ✅ | Kinetica server URL |
| Table Name | Yes | — | ✅ | Target table name |
| Schema | No | — | ✅ | Table schema definition (`col\|Type\|subtype,...`) |
| Avro Schema | No | — | ✅ | Avro JSON schema for auto table creation |
| Batch Size | No | `100` | ✅ | Records per batch |
| Delimiter | No | `,` | ✅ | CSV field delimiter |
| Quote Character | No | `"` | ✅ | CSV quote character |
| File Has Header | No | `true` | ❌ | Whether CSV has header row |
| Skip Errors | No | `false` | ❌ | Route bad records to failure |
| Username | No | — | ✅ | Kinetica username |
| Password | No | — | 🔒 | Kinetica password (sensitive) |
| Date Format | No | — | ✅ | Date parsing format |
| Timezone | No | — | ✅ | Timezone for date parsing |

### PutKineticaFromJSON

| Property | Required | Default | EL | Description |
|----------|----------|---------|-----|-------------|
| Server URL | Yes | — | ✅ | Kinetica server URL |
| Table Name | Yes | — | ✅ | Target table name |
| Schema | No | — | ✅ | Table schema definition (`col\|Type\|subtype,...`) |
| Avro Schema | No | — | ✅ | Avro JSON schema for auto table creation |
| Batch Size | No | `100` | ✅ | Records per batch |
| Username | No | — | ✅ | Kinetica username |
| Password | No | — | 🔒 | Kinetica password (sensitive) |
| Update on Existing PK | No | `false` | ❌ | Update existing primary key records |
| Replicate Table | No | `false` | ❌ | Create replicated table |

### PutKineticaFromAvro

| Property | Required | Default | EL | Description |
|----------|----------|---------|-----|-------------|
| Server URL | Yes | — | ✅ | Kinetica server URL |
| Table Name | Yes | — | ✅ | Target table name |
| Schema | No | — | ✅ | Table schema definition (`col\|Type\|subtype,...`) |
| Avro Schema | No | — | ✅ | Avro JSON schema for auto table creation |
| Batch Size | No | `100` | ✅ | Records per batch |
| Username | No | — | ✅ | Kinetica username |
| Password | No | — | 🔒 | Kinetica password (sensitive) |
| Update on Existing PK | No | `false` | ❌ | Update existing primary key records |
| Replicate Table | No | `false` | ❌ | Create replicated table |

### QueryKineticaToCSV / QueryKineticaToJSON / QueryKineticaToAvro

| Property | Required | Default | EL | Description |
|----------|----------|---------|-----|-------------|
| Server URL | Yes | — | ✅ | Kinetica server URL |
| Table Name | One of Table/SQL | — | ✅ | Full table name (e.g. `schema.table`) |
| SQL Query | One of Table/SQL | — | ✅ | Custom SQL (e.g. `SELECT * FROM t WHERE id > 0`) |
| Batch Size | Yes | `10000` | ✅ | Records per FlowFile batch |
| Delimiter | Yes (CSV only) | `,` | ✅ | CSV delimiter (QueryKineticaToCSV only) |
| Username | No | — | ✅ | Kinetica username |
| Password | No | — | 🔒 | Kinetica password (sensitive) |

> **Note:** Exactly one of `Table Name` or `SQL Query` must be set. Custom validation enforces this.

### GetKineticaToJSON / GetKineticaToCSV / GetKineticaToAvro (ZMQ)

| Property | Required | Description |
|----------|----------|-------------|
| Server URL | Yes | Kinetica server URL |
| Table Name | Yes | Table to monitor |
| Table Monitor URL | Yes | ZeroMQ endpoint (e.g. `tcp://host:9002`) |
| Username | No | Kinetica username |
| Password | No | Kinetica password (sensitive) |
| Delimiter | No | CSV delimiter (GetKineticaToCSV only) |

## Avro Schema for Auto Table Creation

All Put processors accept an **Avro Schema** property (standard Avro JSON format). When the target table does not exist, the processor can auto-create it using the Avro schema to determine column names, types, and properties.

**Resolution priority** (first match wins):
1. Table already exists in Kinetica → use existing schema
2. Pipe-delimited `Schema` property → create table from `col|Type|subtype,...`
3. `Avro Schema` property → create table from Avro JSON schema
4. None provided → attempt column matching from data

**Avro → Kinetica type mapping:**

| Avro Type | Logical Type | Kinetica Type | Column Properties |
|-----------|-------------|---------------|-------------------|
| `string` | — | String | DATA |
| `int` | — | Integer | DATA |
| `long` | — | Long | DATA |
| `float` | — | Float | DATA |
| `double` | — | Double | DATA |
| `boolean` | — | Integer | DATA, INT8 |
| `bytes`/`fixed` | — | ByteBuffer | DATA |
| `long` | `timestamp-millis`/`timestamp-micros` | Long | DATA, TIMESTAMP |
| `int` | `date` | String | DATA, DATE |
| `int`/`long` | `time-millis`/`time-micros` | String | DATA, TIME |
| `bytes` | `decimal` | String | DATA, DECIMAL |
| `["null", X]` | — | *(resolved X)* | + NULLABLE |
| `enum` | — | String | DATA |

**Example Avro Schema** (set in the "Avro Schema" property):
```json
{
  "name": "my_table",
  "type": "record",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": ["null", "string"], "default": null},
    {"name": "value", "type": ["null", "double"], "default": null},
    {"name": "created_at", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}], "default": null},
    {"name": "active", "type": "boolean"}
  ]
}
```

This creates a Kinetica table with columns: `id` (Integer), `name` (String, nullable), `value` (Double, nullable), `created_at` (Long, timestamp, nullable), `active` (Integer, int8).

## Example NiFi Flows

### Ingest CSV

```
GenerateFlowFile → PutKineticaFromCSV → LogAttribute
```

**GenerateFlowFile**: Custom Text = `id,name,value\n1,alpha,1.1\n2,beta,2.2`, MIME Type = `text/csv`
**PutKineticaFromCSV**: Server URL = `http://host:9191`, Table Name = `my_table`, Delimiter = `,`

### Ingest JSON

```
GenerateFlowFile → PutKineticaFromJSON → LogAttribute
```

**GenerateFlowFile**: Custom Text = `[{"id":1,"name":"alpha"},{"id":2,"name":"beta"}]`, MIME Type = `application/json`

### Query Data

```
QueryKineticaToJSON → LogAttribute
```

**QueryKineticaToJSON**: Server URL = `http://host:9191`, SQL Query = `SELECT * FROM demo.my_table WHERE id > 0`, Batch Size = `5000`

### Expected Behaviour
- **Success**: FlowFile routes to `success` with data content and `kinetica.record.count` attribute
- **Failure**: Error FlowFile routes to `failure` with `error.message` attribute

## Environment Variables (.env)

For local testing, create a `.env` file in the project root:

```env
KINETICA_JDBC_URL=http://your-kinetica-host:9191
KINETICA_USERNAME=your_username
KINETICA_PASSWORD=your_password
KINETICA_SCHEMA=demo
KINETICA_TEST_TABLE=nifi_test
NIFI_URL=https://localhost:8443/nifi
NIFI_USERNAME=admin
NIFI_PASSWORD=your_nifi_password
```

> **Never commit this file.** It is listed in `.gitignore`.

## Testing

See [TESTING.md](TESTING.md) for details.

```bash
# Unit tests only (59 tests)
JAVA_HOME=/path/to/java-21 mvn test

# Skip tests for build only
JAVA_HOME=/path/to/java-21 mvn clean package -DskipTests
```

## Build Details

See [BUILD.md](BUILD.md) for detailed build instructions.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ClassNotFoundException: com.gpudb.GPUdb` | Ensure the NAR is in `$NIFI_HOME/extensions/` or `nar_extensions/`, not `lib/` |
| `Connection refused` to Kinetica | Verify the Server URL and that Kinetica is running |
| `Table not found` | Check table name includes schema (e.g. `demo.my_table`) |
| Build fails with Java version error | Ensure `JAVA_HOME` points to a Java 21 JDK |
| ZeroMQ monitor not receiving data | Verify ZMQ ports (9002/9003) are accessible; prefer Query processors instead |
| NAR not detected by NiFi | Restart NiFi after copying NAR; check `$NIFI_HOME/logs/nifi-app.log` |
| Query processor yields with no output | Table may be empty; check Kinetica data directly |
| `Exactly one of Table Name or SQL Query` | Set only one of the two properties on Query processors |

## Links

- [Kinetica Documentation](https://docs.kinetica.com/)
- [Apache NiFi Documentation](https://nifi.apache.org/documentation/)
- [Source Code](https://github.com/kineticadb/kinetica-connector-nifi)
- [Changelog](CHANGELOG.md)
