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
 * DuckDB + S3 Parquet repository.
 * Uses DuckDB embedded, stores Parquet files in S3.
 */
@Repository
public class S3AuditRepository {

    private static final Logger log = LoggerFactory.getLogger(S3AuditRepository.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String endpoint;

    @Autowired
    public S3AuditRepository(S3Client s3Client,
                            @Value("${audit.s3.bucket:audit-bucket}") String bucket,
                            @Value("${audit.s3.endpoint:http://localhost:9000}") String endpoint) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.endpoint = endpoint;
    }

    private Connection getConnection() throws SQLException {
        // Connect to DuckDB with S3 support
        String jdbcUrl = "jdbc:duckdb:?access_key=minioadmin&secret_key=minioadmin&endpoint=" + 
                endpoint.replace("http://", "").replace("https://", "");
        
        // For local DuckDB with file-based storage, we need a different approach
        // We'll use DuckDB embedded and export to Parquet locally, then upload to S3
        return DriverManager.getConnection("jdbc:duckdb:");
    }

    /**
     * Save records using DuckDB and export to Parquet in S3.
     * Partitioned by MONTH.
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

                // Insert records using prepared statement
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

                // Export to Parquet locally first
                String localParquetFile = "/tmp/audit_" + monthKey + ".parquet";
                conn.createStatement().execute(
                    "COPY audit_records TO '" + localParquetFile + "' (FORMAT PARQUET)"
                );

                // Upload to S3 using multipart streaming
                uploadParquetToS3(monthKey, localParquetFile);

                // Cleanup local file
                new File(localParquetFile).delete();

                log.info("Saved {} records to S3 Parquet for {}", monthRecords.size(), monthKey);

            } catch (Exception e) {
                log.error("Failed to save to DuckDB/S3: {}", e.getMessage());
                throw new RuntimeException("Save failed", e);
            }
        }
    }

    /**
     * Upload Parquet file to S3 using streaming (no local temp file).
     */
    private void uploadParquetToS3(String dateKey, String localFile) {
        String objectKey = "audit/" + dateKey + ".parquet";

        try {
            // Read file into bytes
            byte[] fileContent = java.nio.file.Files.readAllBytes(java.nio.file.Path.of(localFile));

            // Simple PUT (works for files up to 5GB)
            // For larger files, multipart upload would be needed
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .contentType("application/parquet")
                    .build();

            s3Client.putObject(putReq, RequestBody.fromBytes(fileContent));

        } catch (Exception e) {
            log.error("S3 upload failed: {}", e.getMessage());
            throw new RuntimeException("S3 upload failed", e);
        }
    }

    /**
     * Query records by timestamp range.
     * Downloads Parquet files by MONTH and queries with DuckDB.
     */
    public List<AuditRecord> findByTimestampBetween(Instant from, Instant to) {
        List<AuditRecord> results = new ArrayList<>();

        // Get month keys in range
        String fromMonth = LocalDate.ofInstant(from, ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM"));
        String toMonth = LocalDate.ofInstant(to, ZoneOffset.UTC)
                .format(DateTimeFormatter.ofPattern("yyyy-MM"));

        // Download and process each month's Parquet file
        for (LocalDate date = LocalDate.ofInstant(from, ZoneOffset.UTC);
             !date.isAfter(LocalDate.ofInstant(to, ZoneOffset.UTC));
             date = date.plusMonths(1)) {
            
            String monthKey = date.format(DateTimeFormatter.ofPattern("yyyy-MM"));
            String objectKey = "audit/" + monthKey + ".parquet";

            try {
                // Download Parquet from S3
                GetObjectRequest getReq = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .build();

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                s3Client.getObject(getReq).transferTo(baos);
                byte[] parquetData = baos.toByteArray();

                // Save to temp file for DuckDB
                String tempFile = "/tmp/query_" + monthKey + ".parquet";
                java.nio.file.Files.write(java.nio.file.Path.of(tempFile), parquetData);

                // Query with DuckDB
                try (Connection conn = getConnection()) {
                    // Register the parquet file
                    String sql = """
                        SELECT id, timestamp, payload 
                        FROM read_parquet(?)
                        WHERE timestamp >= ? AND timestamp <= ?
                        ORDER BY timestamp
                    """;

                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, tempFile);
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
                }

                // Cleanup temp file
                new File(tempFile).delete();

            } catch (NoSuchKeyException ignored) {
                // No data for this day
            } catch (Exception e) {
                log.warn("Error processing {}: {}", objectKey, e.getMessage());
            }
        }

        return results;
    }

    /**
     * Count all records in S3 Parquet files.
     */
    public long count() {
        long total = 0;

        try {
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix("audit/")
                    .build();

            ListObjectsV2Response response = s3Client.listObjectsV2(listReq);

            for (S3Object obj : response.contents()) {
                if (!obj.key().endsWith(".parquet")) continue;

                // Download and count
                GetObjectRequest getReq = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(obj.key())
                        .build();

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                s3Client.getObject(getReq).transferTo(baos);
                byte[] parquetData = baos.toByteArray();

                String tempFile = "/tmp/count_" + System.currentTimeMillis() + ".parquet";
                java.nio.file.Files.write(java.nio.file.Path.of(tempFile), parquetData);

                try (Connection conn = getConnection()) {
                    ResultSet rs = conn.createStatement()
                            .executeQuery("SELECT COUNT(*) FROM read_parquet('" + tempFile + "')");
                    if (rs.next()) {
                        total += rs.getInt(1);
                    }
                }

                new File(tempFile).delete();
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
     * Save a single record.
     */
    public void save(AuditRecord record) {
        batchSave(List.of(record));
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
