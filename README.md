# Kinetica NiFi Connector

Apache NiFi processors for Kinetica database integration.

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

## Building

```bash
# Set Java 21
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

# Build (skip tests - no Kinetica instance required)
mvn clean package -DskipTests

# Run tests
mvn test
```

The NAR file will be created at `nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.0.0.nar`

## Installation

Copy the NAR file to your NiFi installation's `lib/` directory and restart NiFi:

```bash
cp nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.0.0.nar $NIFI_HOME/lib/
```

## Processors

### PutKinetica

Bulk loads FlowFile attributes to Kinetica in batch intervals.

**Properties:**
| Property | Required | Description |
|----------|----------|-------------|
| Server URL | Yes | Kinetica server URL (e.g., `http://localhost:9191`) |
| Table Name | Yes | Target table name |
| Schema | No | Schema definition: `name\|type\|annotation,...` |
| Collection Name | No | Collection for the table |
| Batch Size | No | Records per batch (default: 10000) |
| Username | No | Authentication username |
| Password | No | Authentication password |
| Update on Existing PK | No | Update existing records on PK match |
| Replicate Table | No | Create as replicated table |
| Date Format | No | Date parsing format (e.g., `yyyy-MM-dd HH:mm:ss`) |
| TimeZone | No | Timezone for date parsing |

### PutKineticaFromFile

Bulk loads delimited file contents (CSV, TSV) to Kinetica.

**Additional Properties:**
| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| Delimiter | Yes | `,` | Field delimiter character |
| Quote Character | No | `"` | Quote character for fields |
| Escape Character | No | `"` | Escape character |
| File Has Header | No | `true` | Skip first row as header |
| Skip Errors | Yes | `true` | Continue on parse errors |

**Performance Note:** For large files, chunk into ~1M rows to avoid memory issues.

### GetKineticaToCSV

Monitors a Kinetica table and outputs new records as CSV files.

**Properties:**
| Property | Required | Description |
|----------|----------|-------------|
| Server URL | Yes | Kinetica server URL |
| Table Name | Yes | Table to monitor |
| Table Monitor URL | Yes | ZeroMQ endpoint (e.g., `tcp://localhost:9002`) |
| Delimiter | No | CSV delimiter (default: tab) |
| Username | No | Authentication username |
| Password | No | Authentication password |

### GetKineticaToJSON

Monitors a Kinetica table and outputs new records as JSON.

**Properties:**
| Property | Required | Description |
|----------|----------|-------------|
| Server URL | Yes | Kinetica server URL |
| Table Name | Yes | Table to monitor |
| Table Monitor URL | Yes | ZeroMQ endpoint (e.g., `tcp://localhost:9002`) |
| Username | No | Authentication username |
| Password | No | Authentication password |

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

## Architecture

```
AbstractKineticaProcessor (base)
├── AbstractPutKineticaProcessor (Put operations)
│   ├── PutKinetica
│   └── PutKineticaFromFile
└── AbstractGetKineticaProcessor (Get operations)
    ├── GetKineticaToCSV
    └── GetKineticaToJSON
```

## Documentation

Full documentation available at:
- https://www.kinetica.com/docs/7.2/connectors/nifi_guide.html

## Source Code

- https://github.com/kineticadb/kinetica-connector-nifi

## License

Copyright (c) Kinetica DB Inc.
