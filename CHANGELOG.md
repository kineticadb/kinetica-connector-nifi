# GPUdb NiFi Connector Changelog

## Version 7.2

### Version 7.2.3.0

#### Added — New Processors
-   **PutKineticaFromJSON**: Ingests JSON array or single JSON object into Kinetica via BulkInserter
-   **PutKineticaFromAvro**: Ingests Apache Avro container files into Kinetica via BulkInserter
-   **GetKineticaToAvro**: Monitors a Kinetica table via ZeroMQ and outputs new records as Avro
-   **QueryKineticaToCSV**: Queries a Kinetica table or executes SQL, outputs results as CSV with batching
-   **QueryKineticaToJSON**: Queries a Kinetica table or executes SQL, outputs results as JSON with batching
-   **QueryKineticaToAvro**: Queries a Kinetica table or executes SQL, outputs results as Avro with batching

#### Added — Features
-   **Avro Schema property** on all Put processors for automatic table creation from Avro JSON schema
    -   Supports full Avro-to-Kinetica type mapping: string, int, long, float, double, boolean→int8, bytes, timestamp, date, time, decimal
    -   Nullable detection from Avro union types (`["null", "type"]`)
    -   Logical type support: `timestamp-millis`, `timestamp-micros`, `date`, `time-millis`, `time-micros`, `decimal`
    -   Resolution priority: existing table → pipe-delimited Schema → Avro Schema → fallback
-   Expression Language support (FLOWFILE_ATTRIBUTES scope) on all non-sensitive, non-boolean properties
-   NiFi Parameter Context support (`#{param}`) on all properties
-   Custom validation on Query processors: exactly one of Table Name or SQL Query must be set
-   Provenance events on Query processors (RECEIVE)
-   FlowFile attributes: `kinetica.record.count`, `kinetica.batch.number`, `kinetica.total.records`
-   Unit tests for all 10 processors (59 tests total)
-   Integration tests with automatic skip when no Kinetica available
-   BUILD.md, TESTING.md, comprehensive README.md

#### Changed
-   **NiFi 2.x compatibility**: Upgraded from NiFi 1.3.0 to NiFi 2.7.x
-   **Java 21**: Upgraded from Java 7 to Java 21 (required by NiFi 2.x)
-   **GPUdb API 7.2.3.17**: Upgraded native Kinetica Java API from 7.1.x to 7.2.3.17
-   **Standalone POM**: Replaced `nifi-nar-bundles` parent with standalone Maven POM
-   **NAR packaging**: Updated `nifi-nar-maven-plugin` to 2.3.0
-   **JeroMQ 0.5.4**: Upgraded ZeroMQ library; migrated to `ZContext`/`SocketType.SUB` API
-   **Commons CSV 1.12.0**: Upgraded CSV parser; migrated to builder pattern (`CSVFormat.RFC4180.builder()`)
-   **COLLECTION_NAME removed**: Deprecated parameter removed from `createTable()` calls (Kinetica 7.1+)
-   **Renamed PutKineticaFromFile → PutKineticaFromCSV**: Consistent naming with JSON/Avro processors
-   **BulkInserter optimization**: BulkInserter and WorkerList now created once in `@OnScheduled` and reused across all `onTrigger` calls; `@OnStopped` flushes remaining records and cleans up resources
-   Comprehensive `.gitignore`

#### Removed
-   `log4j:log4j:1.2.17` — replaced with SLF4J (NiFi-provided logging)
-   `com.twitter:hbc-core:2.2.0` — unused Twitter Hosebird dependency
-   `com.google.code.gson:gson:2.2.4` — unused
-   `org.apache.commons:commons-io:1.3.2` — unused
-   `com.jayway.jsonpath:json-path` — unused
-   Old Avro 1.8.1 — now provided transitively by GPUdb API

## Version 7.1

### Version 7.1.0.0 - 2020-07-27

#### Changed
-   Updated version to 7.1


## Version 7.0

### Version 7.0.3.0 - 2019-05-03

#### Changed
-   Updated the Kinetica Java API version to 7.0.3.0 to take advantage of
    recent changes (support for HA failover for multi-head I/O).

### Version 7.0.2.0 - 2019-04-12

#### Changed
-   Modified the PutKineticaFromCSV (formerly PutKineticaFromFile) processor to divert bad records/lines
    from CSV files to a failure relationship.

## Version 6.2

### Version 6.2.0 - 2018-05-16
-   Added the ability to customize the following for the PutKineticaFromCSV
    (formerly PutKineticaFromFile) processor (which loads a CSV file into Kinetica):
    -   Delimiter
    -   Quote character
    -   Escape character


## Version 6.1.0 - 2017-10-05

-   Maintenance


## Version 5.2.0 - 2016-06-25

-   Maintenance.


## Version 5.1.0 - 2016-05-06

-   Updated pom.xml and imports for new GPUdb API structure.


## Version 4.2.0 - 2016-04-11

-   Initial version
