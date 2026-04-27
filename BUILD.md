# Build Guide

## Prerequisites

| Requirement | Version |
|-------------|---------|
| Java JDK | 21+ |
| Apache Maven | 3.8+ |
| Git | any |

## Build Commands

### Full build with tests

```bash
JAVA_HOME=/path/to/java-21 mvn clean package
```

### Build without tests

```bash
JAVA_HOME=/path/to/java-21 mvn clean package -DskipTests
```

### Run unit tests only

```bash
JAVA_HOME=/path/to/java-21 mvn test -pl nifi-GPUdbNiFi-processors
```

## Output

The NAR file is produced at:

```
nifi-GPUdbNiFi-nar/target/nifi-GPUdbNiFi-nar-7.2.3.0.nar
```

## Project Structure

```
kinetica-connector-nifi/
├── pom.xml                          # Root POM with dependency management
├── nifi-GPUdbNiFi-processors/       # Processor implementations (JAR)
│   └── src/main/java/               # 10 NiFi processors + constants
│   └── src/test/java/               # Unit and integration tests (59 tests)
├── nifi-GPUdbNiFi-nar/              # NAR packaging module
│   └── target/*.nar                 # Deployable NiFi archive
└── .env                             # Local credentials (not committed)
```

### Processor Source Files

| File | Processor |
|------|-----------|
| `PutKinetica.java` | Attribute-based ingest |
| `PutKineticaFromFile.java` | CSV/delimited ingest |
| `PutKineticaFromJSON.java` | JSON ingest |
| `PutKineticaFromAvro.java` | Avro ingest |
| `QueryKineticaToCSV.java` | Query → CSV output |
| `QueryKineticaToJSON.java` | Query → JSON output |
| `QueryKineticaToAvro.java` | Query → Avro output |
| `GetKineticaToCSV.java` | ZMQ monitor → CSV |
| `GetKineticaToJSON.java` | ZMQ monitor → JSON |
| `GetKineticaToAvro.java` | ZMQ monitor → Avro |

## Dependencies

| Dependency | Version | Purpose |
|------------|---------|---------|
| NiFi API | 2.7.0 | Core processor API |
| NiFi Utils | 2.7.2 | Test utilities |
| GPUdb Java API | 7.2.3.17 | Native Kinetica client |
| JeroMQ | 0.5.4 | ZeroMQ for table monitoring |
| Commons CSV | 1.12.0 | CSV parsing/writing |
| Jackson Databind | (transitive) | JSON serialization |
| Apache Avro | (transitive) | Avro serialization |

## Troubleshooting

### Java version issues

NiFi 2.x requires Java 21. If your system has multiple Java versions:

```bash
# Check available versions
ls /usr/lib/jvm/

# Set explicitly
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
mvn clean package
```

### Maven compiler errors

The build uses `<source>21</source><target>21</target>` (not `<release>21</release>`) for compatibility with Maven 3.8.x's bundled plexus-compiler.
