package com.pachure.audit;

import com.pachure.audit.model.AuditRecord;
import com.pachure.audit.repository.S3AuditRepository;
import com.pachure.audit.service.AuditService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Benchmark tests for S3-based Audit POC.
 */
@SpringBootTest
class AuditBenchmarkTest {

    @Autowired
    private AuditService auditService;

    @Autowired
    private S3AuditRepository s3Repository;

    // Configuration
    private static final int RECORD_SIZE_BYTES = 2048;
    private static final long TARGET_TOTAL_SIZE_BYTES = 20L * 1024 * 1024 * 1024;
    private static final int RECORDS_PER_HOUR = 20;
    private static final int QUERY_RECORD_COUNT = 200;

    @BeforeEach
    void setUp() {
        auditService.initialize();
    }

    @Test
    void testBulkInsertPerformance() {
        int testRecordCount = 100000;
        
        List<AuditRecord> records = new ArrayList<>(testRecordCount);
        Instant baseTime = Instant.now().minus(5000, ChronoUnit.HOURS);
        
        Map<String, Object> payload = generatePayload(RECORD_SIZE_BYTES);
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < testRecordCount; i++) {
            AuditRecord record = AuditRecord.builder()
                    .id(java.util.UUID.randomUUID().toString())
                    .timestamp(baseTime.plus(i * 1500, ChronoUnit.MILLIS))
                    .payload(new HashMap<>(payload))
                    .build();
            records.add(record);
        }
        
        // Batch insert
        auditService.batchSave(records);
        
        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        
        double recordsPerSecond = (testRecordCount * 1000.0) / durationMs;
        double mbPerSecond = (testRecordCount * RECORD_SIZE_BYTES / (1024.0 * 1024.0)) * 1000.0 / durationMs;
        
        System.out.println("\n=== BULK INSERT BENCHMARK (S3) ===");
        System.out.println("Records inserted: " + testRecordCount);
        System.out.println("Duration: " + durationMs + " ms");
        System.out.println("Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
        System.out.println("Throughput: " + String.format("%.2f", mbPerSecond) + " MB/sec");
        
        long estimatedTotalRecords = TARGET_TOTAL_SIZE_BYTES / RECORD_SIZE_BYTES;
        long estimatedTimeFor20GB = (long) (estimatedTotalRecords / recordsPerSecond / 1000 / 60);
        
        System.out.println("\n--- Extrapolation to 20GB ---");
        System.out.println("Estimated records for 20GB: " + estimatedTotalRecords);
        System.out.println("Estimated time: " + estimatedTimeFor20GB + " minutes");
        
        assertTrue(auditService.count() >= testRecordCount);
    }

    @Test
    void testQueryPerformance() {
        // Insert fresh data for query
        insertTestData(QUERY_RECORD_COUNT * 10);
        
        Instant now = Instant.now();
        Instant from = now.minus(10, ChronoUnit.HOURS);
        Instant to = now;
        
        // Warm-up query
        auditService.findByTimestampBetween(from, to);
        
        long totalQueryTime = 0;
        List<AuditRecord> result = null;
        
        for (int i = 0; i < 5; i++) {
            long start = System.nanoTime();
            result = auditService.findByTimestampBetween(from, to);
            long end = System.nanoTime();
            totalQueryTime += (end - start);
        }
        
        long avgQueryTimeMs = TimeUnit.NANOSECONDS.toMillis(totalQueryTime / 5);
        
        System.out.println("\n=== QUERY BENCHMARK (S3) ===");
        System.out.println("Records returned: " + result.size());
        System.out.println("Average query time: " + avgQueryTimeMs + " ms");
        System.out.println("Time range: " + ChronoUnit.HOURS.between(from, to) + " hours");
        
        assertNotNull(result);
        assertTrue(avgQueryTimeMs < 60000, "Query should complete in under 60 seconds");
    }

    @Test
    void testStorageEfficiency() {
        int recordCount = 10000;
        
        Map<String, Object> payload = generatePayload(RECORD_SIZE_BYTES);
        List<AuditRecord> records = new ArrayList<>(recordCount);
        Instant baseTime = Instant.now();
        
        for (int i = 0; i < recordCount; i++) {
            AuditRecord record = AuditRecord.builder()
                    .id(java.util.UUID.randomUUID().toString())
                    .timestamp(baseTime.plus(i, ChronoUnit.SECONDS))
                    .payload(new HashMap<>(payload))
                    .build();
            records.add(record);
        }
        
        auditService.batchSave(records);
        
        long storageBytes = auditService.getStorageSize();
        long recordCountDb = auditService.count();
        
        double bytesPerRecord = recordCountDb > 0 ? (double) storageBytes / recordCountDb : 0;
        double compressionRatio = bytesPerRecord > 0 ? (double) RECORD_SIZE_BYTES / bytesPerRecord : 0;
        
        System.out.println("\n=== STORAGE EFFICIENCY (S3) ===");
        System.out.println("Records stored: " + recordCountDb);
        System.out.println("Storage size: " + formatBytes(storageBytes));
        System.out.println("Bytes per record: " + String.format("%.2f", bytesPerRecord));
        System.out.println("Compression ratio: " + String.format("%.2f", compressionRatio) + "x");
        
        long estimatedStorageFor20GB = (long) (bytesPerRecord * (TARGET_TOTAL_SIZE_BYTES / RECORD_SIZE_BYTES));
        
        System.out.println("\n--- Extrapolation to 20GB ---");
        System.out.println("Estimated storage for 20GB: " + formatBytes(estimatedStorageFor20GB));
        
        assertTrue(storageBytes > 0);
    }

    private Map<String, Object> generatePayload(int targetBytes) {
        Map<String, Object> payload = new HashMap<>();
        
        String[] actions = {"CREATE", "UPDATE", "DELETE", "READ", "LOGIN", "LOGOUT", "EXPORT", "IMPORT"};
        String[] resources = {"user", "order", "product", "invoice", "document", "session", "payment", "customer"};
        
        String action = actions[(int) (Math.random() * actions.length)];
        String resource = resources[(int) (Math.random() * resources.length)];
        
        payload.put("id", java.util.UUID.randomUUID().toString());
        payload.put("timestamp", Instant.now().toString());
        payload.put("action", action);
        payload.put("resourceType", resource);
        payload.put("resourceId", java.util.UUID.randomUUID().toString().substring(0, 8));
        payload.put("userId", "user-" + (int) (Math.random() * 10000));
        
        if ("CREATE".equals(action) || "UPDATE".equals(action)) {
            Map<String, Object> changes = new HashMap<>();
            int fieldCount = 1 + (int) (Math.random() * 8);
            for (int i = 0; i < fieldCount; i++) {
                changes.put("field_" + i, "value_" + (int) (Math.random() * 1000));
            }
            payload.put("changes", changes);
        } else if ("DELETE".equals(action)) {
            payload.put("softDelete", Math.random() > 0.5);
            payload.put("reason", "reason_" + (int) (Math.random() * 100));
        } else if ("LOGIN".equals(action) || "LOGOUT".equals(action)) {
            payload.put("ipAddress", "192.168." + (int) (Math.random() * 255) + "." + (int) (Math.random() * 255));
            payload.put("userAgent", "Mozilla/5.0-" + java.util.UUID.randomUUID().toString().substring(0, 20));
            payload.put("sessionId", java.util.UUID.randomUUID().toString());
        }
        
        int targetDataSize = 1000 + (int) (Math.random() * 2000);
        
        StringBuilder sb = new StringBuilder();
        while (sb.length() < targetDataSize) {
            sb.append(java.util.UUID.randomUUID().toString().replace("-", ""));
        }
        payload.put("data", sb.substring(0, targetDataSize));
        
        if (Math.random() > 0.3) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("requestId", java.util.UUID.randomUUID().toString());
            metadata.put("correlationId", java.util.UUID.randomUUID().toString());
            metadata.put("clientId", "client-" + (int) (Math.random() * 100));
            metadata.put("environment", Math.random() > 0.5 ? "production" : "staging");
            if (Math.random() > 0.5) {
                metadata.put("extra", java.util.UUID.randomUUID().toString().substring(0, 16));
            }
            payload.put("metadata", metadata);
        }
        
        if (Math.random() > 0.6) {
            payload.put("tags", new String[]{"tag1", "tag2", "tag3"});
        }
        
        return payload;
    }

    private void insertTestData(int count) {
        List<AuditRecord> records = new ArrayList<>(count);
        Instant baseTime = Instant.now().minus(24, ChronoUnit.HOURS);
        
        for (int i = 0; i < count; i++) {
            Instant timestamp = baseTime.plus(i * 180L, ChronoUnit.SECONDS);
            
            int size = 1000 + (int) (Math.random() * 2000);
            Map<String, Object> payload = generatePayload(size);
            
            AuditRecord record = AuditRecord.builder()
                    .id(java.util.UUID.randomUUID().toString())
                    .timestamp(timestamp)
                    .payload(payload)
                    .build();
            records.add(record);
        }
        
        auditService.batchSave(records);
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
