# Pachure Audit POC

Proof of Concept for storing audit data in Apache Parquet format using DuckDB.

## Overview

This POC evaluates the feasibility of storing audit logs with the following requirements:
- **Record size**: ~2KB per record
- **Total volume**: 20GB (~10.5 million records)
- **Query pattern**: Retrieve 200 records by timestamp range
- **Technology**: Java, Spring Boot, DuckDB

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Spring Boot API                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │  Controller  │→ │   Service    │→ │ Repository   │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     DuckDB (Embedded)                       │
│                  Parquet File Storage                       │
│                   ./data/audit/audit.db                     │
└─────────────────────────────────────────────────────────────┘
```

## Data Model

```json
{
  "id": "uuid-string",
  "timestamp": "2024-01-15T10:30:00Z",
  "payload": {
    "event": "audit_event",
    "data": "...", 
    "metadata": {}
  }
}
```

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/audit` | Create audit record |
| GET | `/api/audit?from=&to=` | Query by timestamp range |
| GET | `/api/audit/count` | Get total record count |
| GET | `/api/audit/storage-size` | Get storage size in bytes |
| GET | `/api/audit/health` | Health check |

## Running

```bash
# Build
./mvnw clean package -DskipTests

# Run
java -jar target/audit-poc-1.0.0.jar

# Run tests (includes benchmarks)
./mvnw test
```

## Benchmark Results

### Expected Performance

| Metric | Expected Value |
|--------|---------------|
| **Write throughput** | ~5,000 records/sec |
| **Query time (200 records)** | < 500ms |
| **Storage overhead** | ~10-20% (compression) |
| **20GB insert time** | ~35 minutes |

### Sample Query

```bash
# Query records from last 10 hours
curl "http://localhost:8080/api/audit?from=2024-01-15T00:00:00Z&to=2024-01-15T10:00:00Z"
```

## Configuration

```properties
# application.properties
audit.storage.path=./data/audit
server.port=8080
```

## Key Features

- ✅ Embedded DuckDB (no external dependencies)
- ✅ Parquet storage for compression and query efficiency
- ✅ Timestamp-indexed queries
- ✅ REST API for CRUD operations
- ✅ Benchmark tests for performance validation

## Trade-offs

| Pro | Con |
|-----|-----|
| Embedded (no DB server) | Single-node only |
| Parquet = great compression | Not real-time transactional |
| Fast analytical queries | Update/delete more expensive |
| Low resource overhead | Limited concurrent writes |

## Future Considerations

1. **Cluster mode**: Use DuckDB for multi-node or consider ClickHouse
2. **Real-time**: Add buffering/batching for high-throughput scenarios
3. **Retention**: Implement automatic cleanup of old records
4. **Monitoring**: Add metrics for query latency, storage growth

## License

MIT
