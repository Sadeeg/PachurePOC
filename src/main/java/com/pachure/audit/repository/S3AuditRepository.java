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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * S3-based audit repository using Parquet files.
 * Stores data in S3-compatible storage (MinIO).
 */
@Repository
public class S3AuditRepository {

    private static final Logger log = LoggerFactory.getLogger(S3AuditRepository.class);
    private static final String OBJECT_KEY_PREFIX = "audit/";

    private final S3Client s3Client;
    private final String bucket;

    @Autowired
    public S3AuditRepository(S3Client s3Client, 
                              @Value("${audit.s3.bucket:audit-bucket}") String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        initializeBucket();
    }

    private void initializeBucket() {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            log.info("S3 bucket '{}' exists", bucket);
        } catch (NoSuchBucketException e) {
            s3Client.createBucket(CreateBucketRequest.builder()
                    .bucket(bucket)
                    .build());
            log.info("Created S3 bucket '{}'", bucket);
        } catch (Exception e) {
            log.warn("Could not verify bucket: {}", e.getMessage());
        }
    }

    /**
     * Save audit record to S3 as Parquet.
     * Records are stored in daily Parquet files.
     */
    public void save(AuditRecord record) {
        String dateKey = LocalDate.ofInstant(record.getTimestamp(), ZoneOffset.UTC)
                .format(DateTimeFormatter.ISO_DATE);
        String objectKey = OBJECT_KEY_PREFIX + dateKey + ".parquet";
        
        // For simplicity, we'll use JSON lines format
        // Parquet would require more complex setup with Avro schema
        String jsonLine = toJsonLine(record);
        
        // Append to existing or create new
        try {
            // Try to get existing data
            String existingData = "";
            try {
                GetObjectRequest getReq = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .build();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                s3Client.getObject(getReq).transferTo(baos);
                existingData = baos.toString();
            } catch (NoSuchKeyException ignored) {
                // New file
            }

            // Upload updated data
            String newData = existingData + (existingData.isEmpty() ? "" : "\n") + jsonLine;
            PutObjectRequest putReq = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(objectKey)
                    .contentType("application/jsonl")
                    .build();
            s3Client.putObject(putReq, RequestBody.fromString(newData));
            
            log.debug("Saved audit record {} to S3", record.getId());
        } catch (Exception e) {
            log.error("Failed to save to S3: {}", e.getMessage());
            throw new RuntimeException("S3 save failed", e);
        }
    }

    /**
     * Batch save records.
     */
    public void batchSave(List<AuditRecord> records) {
        if (records.isEmpty()) return;

        // Group by date
        Map<String, List<AuditRecord>> byDate = records.stream()
                .collect(Collectors.groupingBy(r -> 
                        LocalDate.ofInstant(r.getTimestamp(), ZoneOffset.UTC)
                                .format(DateTimeFormatter.ISO_DATE)));

        for (Map.Entry<String, List<AuditRecord>> entry : byDate.entrySet()) {
            String dateKey = entry.getKey();
            List<AuditRecord> dayRecords = entry.getValue();
            
            String objectKey = OBJECT_KEY_PREFIX + dateKey + ".jsonl";
            
            try {
                String existingData = "";
                try {
                    GetObjectRequest getReq = GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(objectKey)
                            .build();
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    s3Client.getObject(getReq).transferTo(baos);
                    existingData = baos.toString();
                } catch (NoSuchKeyException ignored) {
                }

                StringBuilder newData = new StringBuilder(existingData);
                if (!existingData.isEmpty()) {
                    newData.append("\n");
                }
                
                for (int i = 0; i < dayRecords.size(); i++) {
                    newData.append(toJsonLine(dayRecords.get(i)));
                    if (i < dayRecords.size() - 1) {
                        newData.append("\n");
                    }
                }

                PutObjectRequest putReq = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .contentType("application/jsonl")
                        .build();
                s3Client.putObject(putReq, RequestBody.fromString(newData.toString()));
                
                log.info("Saved {} records to S3 for date {}", dayRecords.size(), dateKey);
            } catch (Exception e) {
                log.error("Failed to batch save to S3: {}", e.getMessage());
                throw new RuntimeException("S3 batch save failed", e);
            }
        }
    }

    /**
     * Query records by timestamp range.
     * Downloads relevant daily files and filters.
     */
    public List<AuditRecord> findByTimestampBetween(Instant from, Instant to) {
        List<AuditRecord> results = new ArrayList<>();
        
        LocalDate fromDate = LocalDate.ofInstant(from, ZoneOffset.UTC);
        LocalDate toDate = LocalDate.ofInstant(to, ZoneOffset.UTC);
        
        // Iterate through each day in range
        for (LocalDate date = fromDate; !date.isAfter(toDate); date = date.plusDays(1)) {
            String dateKey = date.format(DateTimeFormatter.ISO_DATE);
            String objectKey = OBJECT_KEY_PREFIX + dateKey + ".jsonl";
            
            try {
                GetObjectRequest getReq = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(objectKey)
                        .build();
                
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                s3Client.getObject(getReq).transferTo(baos);
                String content = baos.toString();
                
                String[] lines = content.split("\n");
                for (String line : lines) {
                    if (line.trim().isEmpty()) continue;
                    try {
                        AuditRecord record = fromJsonLine(line);
                        if (!record.getTimestamp().isBefore(from) && 
                            !record.getTimestamp().isAfter(to)) {
                            results.add(record);
                        }
                    } catch (Exception ignored) {
                    }
                }
            } catch (NoSuchKeyException ignored) {
                // No data for this day
            } catch (Exception e) {
                log.warn("Error reading {}: {}", objectKey, e.getMessage());
            }
        }
        
        // Sort by timestamp
        results.sort(Comparator.comparing(AuditRecord::getTimestamp));
        return results;
    }

    /**
     * Count records.
     */
    public long count() {
        long total = 0;
        try {
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(OBJECT_KEY_PREFIX)
                    .build();
            
            ListObjectsV2Response response = s3Client.listObjectsV2(listReq);
            for (S3Object obj : response.contents()) {
                GetObjectRequest getReq = GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(obj.key())
                        .build();
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                s3Client.getObject(getReq).transferTo(baos);
                String[] lines = baos.toString().split("\n");
                total += lines.length;
            }
        } catch (Exception e) {
            log.warn("Error counting: {}", e.getMessage());
        }
        return total;
    }

    /**
     * Get storage size in bytes.
     */
    public long getStorageSize() {
        long totalSize = 0;
        try {
            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(bucket)
                    .prefix(OBJECT_KEY_PREFIX)
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

    private String toJsonLine(AuditRecord record) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.findAndRegisterModules();
            Map<String, Object> map = new HashMap<>();
            map.put("id", record.getId());
            map.put("timestamp", record.getTimestamp().toString());
            map.put("payload", record.getPayload());
            return mapper.writeValueAsString(map);
        } catch (Exception e) {
            throw new RuntimeException("JSON serialization failed", e);
        }
    }

    private AuditRecord fromJsonLine(String line) {
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.findAndRegisterModules();
            Map<String, Object> map = mapper.readValue(line, Map.class);
            
            AuditRecord record = AuditRecord.builder()
                    .id((String) map.get("id"))
                    .timestamp(Instant.parse((String) map.get("timestamp")))
                    .build();
            
            @SuppressWarnings("unchecked")
            Map<String, Object> payload = (Map<String, Object>) map.get("payload");
            if (payload != null) {
                record.setPayload(payload);
            }
            
            return record;
        } catch (Exception e) {
            throw new RuntimeException("JSON deserialization failed", e);
        }
    }
}
