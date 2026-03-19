package com.pachure.audit;

import com.pachure.audit.model.AuditRecord;
import com.pachure.audit.repository.AuditRepository;
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
 * Benchmark tests for the Audit POC.
 * 
 * Tests:
 * 1. Write performance: 20GB worth of 2KB records
 * 2. Query performance: Retrieve 200 records by timestamp
 * 3. Storage efficiency: Actual storage vs expected
 */
@SpringBootTest
class AuditBenchmarkTest {

    @Autowired
    private AuditService auditService;

    @Autowired
    private AuditRepository auditRepository;

    // Configuration
    private static final int RECORD_SIZE_BYTES = 2048; // 2KB
    private static final long TARGET_TOTAL_SIZE_BYTES = 20L * 1024 * 1024 * 1024; // 20GB
    private static final int RECORDS_PER_HOUR = 20;
    private static final int QUERY_RECORD_COUNT = 200;

    /**
     * Initialize the audit table before each test.
     */
    @BeforeEach
    void setUp() {
        auditService.initialize();
    }

    /**
     * Test 1: Bulk insert performance
     * 
     * Calculate how many records we need for ~20GB:
     * 20GB / 2KB = 10,485,760 records
     * 
     * For practical testing, we'll use a smaller subset
     * but extrapolate the results.
     */
    @Test
    void testBulkInsertPerformance() {
        // For 20GB: ~10.5M records
        // For testing: use 100,000 records and extrapolate
        int testRecordCount = 100000;
        
        // Generate test records
        List<AuditRecord> records = new ArrayList<>(testRecordCount);
        Instant baseTime = Instant.now().minus(5000, ChronoUnit.HOURS);
        
        // Generate 2KB payload
        Map<String, Object> payload = generatePayload(RECORD_SIZE_BYTES);
        
        long startTime = System.nanoTime();
        
        for (int i = 0; i < testRecordCount; i++) {
            AuditRecord record = AuditRecord.builder()
                    .id(java.util.UUID.randomUUID().toString())
                    .timestamp(baseTime.plus(i * 1500, ChronoUnit.MILLIS)) // 20 records/hour
                    .payload(new HashMap<>(payload))
                    .build();
            records.add(record);
        }
        
        // Batch insert
        auditRepository.batchInsert(records);
        
        long endTime = System.nanoTime();
        long durationMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
        
        // Calculate throughput
        double recordsPerSecond = (testRecordCount * 1000.0) / durationMs;
        double mbPerSecond = (testRecordCount * RECORD_SIZE_BYTES / (1024.0 * 1024.0)) * 1000.0 / durationMs;
        
        System.out.println("\n=== BULK INSERT BENCHMARK ===");
        System.out.println("Records inserted: " + testRecordCount);
        System.out.println("Duration: " + durationMs + " ms");
        System.out.println("Throughput: " + String.format("%.2f", recordsPerSecond) + " records/sec");
        System.out.println("Throughput: " + String.format("%.2f", mbPerSecond) + " MB/sec");
        
        // Extrapolate to 20GB
        long estimatedTotalRecords = TARGET_TOTAL_SIZE_BYTES / RECORD_SIZE_BYTES;
        long estimatedTimeFor20GB = (long) (estimatedTotalRecords / recordsPerSecond / 1000 / 60);
        
        System.out.println("\n--- Extrapolation to 20GB ---");
        System.out.println("Estimated records for 20GB: " + estimatedTotalRecords);
        System.out.println("Estimated time: " + estimatedTimeFor20GB + " minutes");
        
        // Verify insert
        assertTrue(auditRepository.count() >= testRecordCount);
    }

    /**
     * Test 2: Query performance for timestamp range
     * 
     * Query 200 records over a time range.
     */
    @Test
    void testQueryPerformance() {
        // First, insert some data if not present
        if (auditRepository.count() < QUERY_RECORD_COUNT) {
            insertTestData(QUERY_RECORD_COUNT * 10);
        }
        
        // Determine time range that should return ~200 records
        // 20 records/hour = 1 record every 3 minutes
        // 200 records = 10 hours range
        Instant now = Instant.now();
        Instant from = now.minus(10, ChronoUnit.HOURS);
        Instant to = now;
        
        // Warm-up query
        auditRepository.findByTimestampBetween(from, to);
        
        // Measure query time (average of 5 runs)
        long totalQueryTime = 0;
        List<AuditRecord> result = null;
        
        for (int i = 0; i < 5; i++) {
            long start = System.nanoTime();
            result = auditRepository.findByTimestampBetween(from, to);
            long end = System.nanoTime();
            totalQueryTime += (end - start);
        }
        
        long avgQueryTimeMs = TimeUnit.NANOSECONDS.toMillis(totalQueryTime / 5);
        
        System.out.println("\n=== QUERY BENCHMARK ===");
        System.out.println("Records returned: " + result.size());
        System.out.println("Average query time: " + avgQueryTimeMs + " ms");
        System.out.println("Time range: " + ChronoUnit.HOURS.between(from, to) + " hours");
        
        // Verify reasonable query time
        assertNotNull(result);
        assertTrue(avgQueryTimeMs < 5000, "Query should complete in under 5 seconds");
    }

    /**
     * Test 3: Storage efficiency
     * 
     * Measure actual storage consumption.
     */
    @Test
    void testStorageEfficiency() {
        // Insert a known number of records
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
        
        auditRepository.batchInsert(records);
        
        // Get storage size
        long storageBytes = auditRepository.getStorageSize();
        long recordCountDb = auditRepository.count();
        
        double bytesPerRecord = (double) storageBytes / recordCountDb;
        double compressionRatio = (double) RECORD_SIZE_BYTES / bytesPerRecord;
        
        System.out.println("\n=== STORAGE EFFICIENCY ===");
        System.out.println("Records stored: " + recordCountDb);
        System.out.println("Storage size: " + formatBytes(storageBytes));
        System.out.println("Bytes per record: " + String.format("%.2f", bytesPerRecord));
        System.out.println("Compression ratio: " + String.format("%.2f", compressionRatio) + "x");
        
        // Extrapolate to 20GB
        long estimatedStorageFor20GB = (long) (bytesPerRecord * (TARGET_TOTAL_SIZE_BYTES / RECORD_SIZE_BYTES));
        
        System.out.println("\n--- Extrapolation to 20GB ---");
        System.out.println("Estimated storage for 20GB: " + formatBytes(estimatedStorageFor20GB));
        
        assertTrue(storageBytes > 0);
    }

    /**
     * Helper: Generate a payload of approximately the target size.
     */
    private Map<String, Object> generatePayload(int targetBytes) {
        Map<String, Object> payload = new HashMap<>();
        
        // Estimate overhead for JSON serialization
        // Add padding fields to reach target size
        StringBuilder sb = new StringBuilder();
        while (sb.length() < targetBytes - 100) {
            sb.append("x");
        }
        
        payload.put("event", "audit_event_" + System.currentTimeMillis());
        payload.put("data", sb.toString());
        payload.put("metadata", Map.of(
            "source", "poc_test",
            "version", "1.0"
        ));
        
        return payload;
    }

    /**
     * Helper: Insert test data for query testing.
     */
    private void insertTestData(int count) {
        Map<String, Object> payload = generatePayload(RECORD_SIZE_BYTES);
        List<AuditRecord> records = new ArrayList<>(count);
        Instant baseTime = Instant.now().minus(24, ChronoUnit.HOURS);
        
        for (int i = 0; i < count; i++) {
            AuditRecord record = AuditRecord.builder()
                    .id(java.util.UUID.randomUUID().toString())
                    .timestamp(baseTime.plus(i * 180, ChronoUnit.MILLIS)) // 20/hour
                    .payload(new HashMap<>(payload))
                    .build();
            records.add(record);
        }
        
        auditRepository.batchInsert(records);
    }

    /**
     * Helper: Format bytes to human readable.
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }
}
