package com.pachure.audit.repository;

import com.pachure.audit.model.AuditRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * DuckDB + S3 Parquet repository with streaming.
 * Uses DuckDB for Parquet generation, streams directly to S3.
 */
@Repository
public class S3AuditRepository {

    private static final Logger log = LoggerFactory.getLogger(S3AuditRepository.class);

    private final S3Client s3Client;
    private final String bucket;

    @Autowired
    public S3AuditRepository(S3Client s3Client,
                            @Value("${audit.s3.bucket:audit-bucket}") String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:duckdb:");
    }

    /**
     * Save records using DuckDB and export directly to S3.
     * Uses DuckDB's S3 extension for streaming write.
     */
    public void batchSave(List<AuditRecord> records) {
        if (records.isEmpty()) return;

        // Group by month (YYYY-MM)
        Map<String, List<AuditRecord>> byMonth = records.stream()
                .collect(Collectors.groupingBy(r ->
                        LocalDate.ofInstant(r.getTimestamp(), ZoneOffset.UTC)
                                .format(DateTimeFormatter.ofPattern("yyyy-MM"))));

        for (Map.Entry<String, List<AuditRecord>> entry : byMonth.entrySet()) {
            String monthKey = entry.getKey();
            List<AuditRecord> monthRecords = entry.getValue();

            try (Connection conn = getConnection()) {
                // Create table
                conn.createStatement().execute("""
                    CREATE TABLE IF NOT EXISTS audit_records (
                        id VARCHAR,
                        timestamp TIMESTAMP,
                        payload VARCHAR
                    )
                """);

                // Insert records
                String sql = "INSERT INTO audit_records (id, timestamp, payload) VALUES (?, ?, ?)";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    for (AuditRecord record : monthRecords) {
                        ps.setString(1, record.getId());
                        ps.setTimestamp(2, record.getTimestamp() != null ?
                                Timestamp.from(record.getTimestamp()) : Timestamp.from(Instant.now()));
                        ps.setString(3, record.getPayloadAsJson());
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }

                // Load httpfs extension and configure for MinIO
                conn.createStatement().execute("INSTALL httpfs;");
                conn.createStatement().execute("LOAD httpfs");
                
                // Configure MinIO S3 settings
                conn.createStatement().execute("SET s3_endpoint='localhost:9000'");
                conn.createStatement().execute("SET s3_access_key_id='minioadmin'");
                conn.createStatement().execute("SET s3_secret_access_key='minioadmin'");
                conn.createStatement().execute("SET s3_use_ssl=false");
                conn.createStatement().execute("SET s3_region='us-east-1'");
                // Force path-style addressing for MinIO
                conn.createStatement().execute("SET s3_url_style='path'");

                // Now try native S3 export
                String s3Path = String.format("s3://%s/audit/%s.parquet", bucket, monthKey);

                try {
                    conn.createStatement().execute(
                        "COPY audit_records TO '" + s3Path + "' (FORMAT PARQUET)"
                    );
                    log.info("Native S3 export successful for {}", monthKey);
                } catch (SQLException e) {
                    // Fallback to Java streaming
                    log.warn("Native S3 failed, using Java stream: {}", e.getMessage());
                    
                    String localParquetFile = "/tmp/stream_" + monthKey + ".parquet";
                    conn.createStatement().execute(
                        "COPY audit_records TO '" + localParquetFile + "' (FORMAT PARQUET)"
                    );
                    streamToS3(monthKey, localParquetFile);
                    new File(localParquetFile).delete();
                }

                log.info("Saved {} records to S3 Parquet for {}", monthRecords.size(), monthKey);

            } catch (Exception e) {
                log.error("Failed to save: {}", e.getMessage());
                throw new RuntimeException("Save failed", e);
            }
        }
    }

    /**
     * Stream Parquet file to S3 efficiently.
     */
    private void streamToS3(String monthKey, String localFile) {
        String objectKey = "audit/" + monthKey + ".parquet";

        try {
            // Use FileInputStream directly - AWS SDK handles buffering
            File file = new File(localFile);
            long contentLength = file.length();

            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .contentType("application/parquet")
                    .contentLength(contentLength)
                    .build();

            // Stream directly from file to S3 (no loading into memory)
            try (FileInputStream fis = new FileInputStream(file)) {
                s3Client.putObject(putReq, RequestBody.fromInputStream(fis, contentLength));
            }

            log.debug("Streamed {} to S3 ({} bytes)", objectKey, contentLength);

        } catch (Exception e) {
            log.error("S3 stream failed: {}", e.getMessage());
            throw new RuntimeException("S3 stream failed", e);
        }
    }

    /**
     * Save a single record.
     */
    public void save(AuditRecord record) {
        batchSave(List.of(record));
    }

    /**
     * Query records by timestamp range.
     * Uses DuckDB's S3 streaming - no file download needed!
     */
    public List<AuditRecord> findByTimestampBetween(Instant from, Instant to) {
        List<AuditRecord> results = new ArrayList<>();

        LocalDate fromDate = LocalDate.ofInstant(from, ZoneOffset.UTC);
        LocalDate toDate = LocalDate.ofInstant(to, ZoneOffset.UTC);

        // Process each month's Parquet file using S3 streaming
        for (LocalDate date = fromDate; !date.isAfter(toDate); date = date.plusMonths(1)) {
            
            String monthKey = date.format(DateTimeFormatter.ofPattern("yyyy-MM"));

            try (Connection conn = getConnection()) {
                // Configure S3 for MinIO
                conn.createStatement().execute("INSTALL httpfs;");
                conn.createStatement().execute("LOAD httpfs");
                conn.createStatement().execute("SET s3_endpoint='localhost:9000'");
                conn.createStatement().execute("SET s3_access_key_id='minioadmin'");
                conn.createStatement().execute("SET s3_secret_access_key='minioadmin'");
                conn.createStatement().execute("SET s3_use_ssl=false");
                conn.createStatement().execute("SET s3_region='us-east-1'");
                conn.createStatement().execute("SET s3_url_style='path'");

                // Direct S3 streaming query - no download!
                String s3Path = String.format(
                    "s3://%s/audit/%s.parquet", bucket, monthKey);

                String sql = """
                    SELECT id, timestamp, payload 
                    FROM read_parquet(?)
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp
                """;

                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, s3Path);
                    ps.setTimestamp(2, Timestamp.from(from));
                    ps.setTimestamp(3, Timestamp.from(to));

                    ResultSet rs = ps.executeQuery();
                    while (rs.next()) {
                        AuditRecord record = AuditRecord.builder()
                                .id(rs.getString("id"))
                                .timestamp(rs.getTimestamp("timestamp").toInstant())
                                .build();
                        record.setPayloadFromJson(rs.getString("payload"));
                        results.add(record);
                    }
                }

            } catch (SQLException ignored) {
                // No data for this month
            } catch (Exception e) {
                log.warn("Error processing {}: {}", monthKey, e.getMessage());
            }
        }

        return results;
    }

    /**
     * Count all records using S3 streaming.
     */
    public long count() {
        long total = 0;

        try {
            // List all Parquet files in S3
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix("audit/")
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listReq);
            List<String> parquetFiles = new ArrayList<>();
            
            for (S3Object obj : response.contents()) {
                if (obj.key().endsWith(".parquet")) {
                    // Extract month key from filename
                    String key = obj.key(); // e.g., "audit/2025-08.parquet"
                    String monthKey = key.replace("audit/", "").replace(".parquet", "");
                    parquetFiles.add(monthKey);
                }
            }

            // Count using S3 streaming - no download needed
            try (Connection conn = getConnection()) {
                // Configure S3
                conn.createStatement().execute("INSTALL httpfs;");
                conn.createStatement().execute("LOAD httpfs");
                conn.createStatement().execute("SET s3_endpoint='localhost:9000'");
                conn.createStatement().execute("SET s3_access_key_id='minioadmin'");
                conn.createStatement().execute("SET s3_secret_access_key='minioadmin'");
                conn.createStatement().execute("SET s3_use_ssl=false");
                conn.createStatement().execute("SET s3_region='us-east-1'");
                conn.createStatement().execute("SET s3_url_style='path'");

                for (String monthKey : parquetFiles) {
                    try {
                        String s3Path = String.format("s3://%s/audit/%s.parquet", bucket, monthKey);
                        ResultSet rs = conn.createStatement()
                                .executeQuery("SELECT COUNT(*) FROM read_parquet('" + s3Path + "')");
                        if (rs.next()) {
                            total += rs.getInt(1);
                        }
                    } catch (SQLException ignored) {
                        // File might not exist
                    }
                }
            }

        } catch (Exception e) {
            log.warn("Error counting: {}", e.getMessage());
        }

        return total;
    }

    /**
     * Get total storage size.
     */
    public long getStorageSize() {
        long totalSize = 0;

        try {
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix("audit/")
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listReq);

            for (S3Object obj : response.contents()) {
                totalSize += obj.size();
            }

        } catch (Exception e) {
            log.warn("Error getting storage size: {}", e.getMessage());
        }

        return totalSize;
    }

    /**
     * Initialize - create bucket if needed.
     */
    public void initialize() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (NoSuchBucketException e) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        } catch (Exception e) {
            log.warn("Could not verify bucket: {}", e.getMessage());
        }
    }
}
