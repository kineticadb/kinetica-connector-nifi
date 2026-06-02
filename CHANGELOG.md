# GPUdb NiFi Connector Changelog

## Version 7.2

### Version 7.2.0.1 - 2026-06-02

#### Added
-   Avro schema-driven table creation
-   Disable auto-discovery & failover connection parameters
-   Streaming/paging support for queries
-   Pre-built NAR file under `dist`

#### Changed
-   Upgraded from NiFi 2.0.0 to NiFi 2.7.0
-   Upgraded to Jackson 2.18.6
-   Upgraded to Avro 1.11.4
-   Common properties now configurable per-FlowFile


### Version 7.2.0.0 - 2026-05-31

#### Added
-   New processors:
    -   `ExecuteKineticaSQL`
    -   `ListKineticaTables`
    -   `PutKineticaFromAvro`
    -   `PutKineticaFromJSON`
    -   `PutKineticaRecord`
    -   `QueryKineticaToAvro`
    -   `QueryKineticaToCSV`
    -   `QueryKineticaToJSON`
-   SSL/TLS support for secure connections
-   Connection pooling for improved performance
-   Configurable timeouts for connection and socket operations
-   Expression Language support for dynamic configuration

#### Changed
-   Upgraded from NiFi 1.3.0 to NiFi 2.0.0
-   Upgraded from Java 7 to Java 21
-   Refactored package structure from
    `com.gisfederal.gpudb.processors.GPUdbNiFi` to
    `com.kinetica.nifi.processors`
-   Moved to 3-tier processor inheritance hierarchy for maintainability
-   Update README with comprehensive documentation

#### Fixed
-   CSV parser performance issue (10-100x improvement)
-   ZeroMQ connection leak in Get processors
-   Table name validation to prevent SQL injection
-   Null-safe utility methods



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
-   Modified the PutKineticaFromFile processor to divert bad records/lines
    from CSV files to a failure relationship.



## Version 6.2

### Version 6.2.0 - 2018-05-16
-   Added the ability to customize the following for the PutKineticaFromFile
    processor (which loads a CSV file into Kinetica):
    -   Delimiter
    -   Quote character
    -   Escape character



## Version 6.1 - 2017-10-05

-   Maintenance



## Version 5.2 - 2016-06-25

-   Maintenance.



## Version 5.1 - 2016-05-06

-   Updated pom.xml and imports for new GPUdb API structure.



## Version 4.2 - 2016-04-11

-   Initial version
