# Testing Guide

## Test Categories

### Unit Tests (no live Kinetica required)

| Test Class | Tests | Description |
|------------|-------|-------------|
| `TestPutKineticaProperties` | 10 | PutKinetica property validation |
| `TestPutKineticaFromCSVProperties` | 10 | PutKineticaFromCSV property validation |
| `TestGetKineticaProperties` | 8 | GetKineticaToJSON/CSV property validation |
| `TestPutKineticaFromJsonAvroProperties` | 10 | PutKineticaFromJSON/Avro property validation |
| `TestQueryKineticaProperties` | 19 | QueryKineticaToCSV/JSON/Avro property validation |

### Integration Tests (require live Kinetica)

| Test Class | Tests | Description |
|------------|-------|-------------|
| `TestKineticaIntegration` | 2 | End-to-end insert and verify via GPUdb API |
| `TestPutKineticaFromCSV` | 2 | CSV import with success/failure paths (legacy) |

Integration tests **skip automatically** when no Kinetica instance is available.

## Running Tests

### Unit tests only

```bash
JAVA_HOME=/path/to/java-21 mvn test -pl nifi-GPUdbNiFi-processors \
  -Dtest="TestPutKineticaProperties,TestPutKineticaFromCSVProperties,TestGetKineticaProperties,TestPutKineticaFromJsonAvroProperties,TestQueryKineticaProperties"
```

### All tests (integration tests skip if no server)

```bash
JAVA_HOME=/path/to/java-21 mvn test -pl nifi-GPUdbNiFi-processors
```

### Integration tests with a live Kinetica

Option 1: Use a `.env` file in the project root:

```env
KINETICA_JDBC_URL=http://your-kinetica-host:9191
KINETICA_USERNAME=your_username
KINETICA_PASSWORD=your_password
KINETICA_SCHEMA=demo
```

Option 2: Use environment variables:

```bash
export KINETICA_JDBC_URL=http://your-kinetica-host:9191
export KINETICA_USERNAME=your_username
export KINETICA_PASSWORD=your_password
JAVA_HOME=/path/to/java-21 mvn test -pl nifi-GPUdbNiFi-processors
```

Option 3: Use the legacy `-Durl` system property for `TestPutKineticaFromCSV`:

```bash
JAVA_HOME=/path/to/java-21 mvn test -pl nifi-GPUdbNiFi-processors -Durl=http://host:9191
```

## Integration Test Behaviour

- Tests create temporary tables with random names (`nifi_test_<uuid>`)
- Tables are cleaned up after each test via `@After` methods
- Tests never drop non-test tables
- If cleanup fails, orphan tables can be removed manually

## Test Results Summary

```
Tests run: 59, Failures: 0, Errors: 0, Skipped: 0
```

- 57 unit tests: PASS
- 2 integration tests: PASS (when Kinetica available) / SKIP (when not)

## NiFi End-to-End Test Results

All 10 processors verified in NiFi 2.7.2:

| Processor | Status | Notes |
|-----------|--------|-------|
| PutKineticaFromCSV | ✅ Verified | 3 rows ingested |
| PutKineticaFromJSON | ✅ Verified | 3 rows ingested |
| PutKineticaFromAvro | ✅ Verified | 3 rows ingested |
| PutKinetica (attributes) | ✅ Verified | 1 row ingested |
| QueryKineticaToCSV | ✅ Verified | Table + SQL modes, batching works |
| QueryKineticaToJSON | ✅ Verified | SQL mode, all 3 rows returned |
| QueryKineticaToAvro | ✅ Verified | Table mode, batching works |
| GetKineticaToCSV | ✅ Loaded | ZMQ ports required (not tested E2E) |
| GetKineticaToJSON | ✅ Loaded | ZMQ ports required (not tested E2E) |
| GetKineticaToAvro | ✅ Loaded | ZMQ ports required (not tested E2E) |
