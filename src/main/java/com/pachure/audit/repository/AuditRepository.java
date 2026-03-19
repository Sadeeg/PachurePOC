package com.pachure.audit.repository;

import com.pachure.audit.model.AuditRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Repository for audit record operations using DuckDB with Parquet storage.
 * 
 * DuckDB can read/write Parquet files directly and provides excellent
 * performance for analytical queries on large datasets.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class AuditRepository {

    private final JdbcTemplate jdbcTemplate;

    /**
     * Initialize the audit table.
     * Uses Parquet format for efficient storage and querying.
     */
    public void initializeTable() {
        // Create table - DuckDB will handle Parquet internally
        String createTableSql = """
            CREATE TABLE IF NOT EXISTS audit_records (
                id VARCHAR(36) PRIMARY KEY,
                timestamp TIMESTAMP,
                payload VARCHAR
            )
            """;
        
        String createIndexSql = """
            CREATE INDEX IF NOT EXISTS idx_timestamp 
            ON audit_records(timestamp)
            """;
        
        jdbcTemplate.execute(createTableSql);
        jdbcTemplate.execute(createIndexSql);
        
        log.info("Audit table initialized");
    }

    /**
     * Insert a single audit record.
     */
    public void insert(AuditRecord record) {
        String sql = """
            INSERT INTO audit_records (id, timestamp, payload) 
            VALUES (?, ?, ?)
            ON CONFLICT (id) DO UPDATE SET
                timestamp = excluded.timestamp,
                payload = excluded.payload
            """;
        
        jdbcTemplate.update(sql, 
            record.getId() != null ? record.getId() : UUID.randomUUID().toString(),
            record.getTimestamp() != null ? record.getTimestamp() : Instant.now(),
            record.getPayloadAsJson()
        );
    }

    /**
     * Batch insert audit records for performance.
     */
    public void batchInsert(List<AuditRecord> records) {
        String sql = """
            INSERT INTO audit_records (id, timestamp, payload) 
            VALUES (?, ?, ?)
            """;
        
        jdbcTemplate.batchUpdate(sql, records, records.size(), (ps, record) -> {
            ps.setString(1, record.getId() != null ? record.getId() : UUID.randomUUID().toString());
            ps.setObject(2, record.getTimestamp() != null ? record.getTimestamp() : Instant.now());
            ps.setString(3, record.getPayloadAsJson());
        });
        
        log.debug("Inserted {} records", records.size());
    }

    /**
     * Query records by timestamp range.
     * This is the primary use case for audit logs.
     * 
     * @param from Start of time range (inclusive)
     * @param to End of time range (inclusive)
     * @return List of audit records in the time range
     */
    public List<AuditRecord> findByTimestampBetween(Instant from, Instant to) {
        String sql = """
            SELECT id, timestamp, payload 
            FROM audit_records 
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp ASC
            """;
        
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            AuditRecord record = AuditRecord.builder()
                .id(rs.getString("id"))
                .timestamp(rs.getTimestamp("timestamp").toInstant())
                .build();
            record.setPayloadFromJson(rs.getString("payload"));
            return record;
        }, from, to);
    }

    /**
     * Count records in a time range - useful for pagination.
     */
    public long countByTimestampBetween(Instant from, Instant to) {
        String sql = """
            SELECT COUNT(*) 
            FROM audit_records 
            WHERE timestamp >= ? AND timestamp <= ?
            """;
        
        Long count = jdbcTemplate.queryForObject(sql, Long.class, from, to);
        return count != null ? count : 0;
    }

    /**
     * Get total record count.
     */
    public long count() {
        String sql = "SELECT COUNT(*) FROM audit_records";
        Long count = jdbcTemplate.queryForObject(sql, Long.class);
        return count != null ? count : 0;
    }

    /**
     * Export to Parquet file for external processing.
     */
    public void exportToParquet(String path) {
        String sql = String.format("""
            COPY audit_records TO '%s' 
            (FORMAT PARQUET, OVERWRITE TRUE)
            """, path);
        
        jdbcTemplate.execute(sql);
        log.info("Exported to Parquet: {}", path);
    }

    /**
     * Get storage size in bytes.
     */
    public long getStorageSize() {
        String path = "data/audit";
        try {
            java.nio.file.Path p = java.nio.file.Path.of(path);
            if (java.nio.file.Files.exists(p)) {
                return java.nio.file.Files.walk(p)
                    .filter(java.nio.file.Files::isRegularFile)
                    .mapToLong(pf -> {
                        try {
                            return java.nio.file.Files.size(pf);
                        } catch (Exception e) {
                            return 0L;
                        }
                    })
                    .sum();
            }
        } catch (Exception e) {
            log.warn("Could not calculate storage size", e);
        }
        return 0;
    }
}
