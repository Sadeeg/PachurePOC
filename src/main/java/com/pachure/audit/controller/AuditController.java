package com.pachure.audit.controller;

import com.pachure.audit.model.AuditRecord;
import com.pachure.audit.service.AuditService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

/**
 * REST controller for audit log operations.
 */
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditService auditService;

    /**
     * Create a new audit record.
     * POST /api/audit
     * Body: { "key": "value", ... }
     */
    @PostMapping
    public ResponseEntity<AuditRecord> createAuditRecord(@RequestBody Map<String, Object> payload) {
        AuditRecord record = auditService.createAuditRecord(payload);
        return ResponseEntity.ok(record);
    }

    /**
     * Query audit records by time range.
     * GET /api/audit?from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z
     */
    @GetMapping
    public ResponseEntity<List<AuditRecord>> queryAuditRecords(
            @RequestParam Instant from,
            @RequestParam Instant to) {
        
        List<AuditRecord> records = auditService.queryByTimeRange(from, to);
        return ResponseEntity.ok(records);
    }

    /**
     * Get total record count.
     * GET /api/audit/count
     */
    @GetMapping("/count")
    public ResponseEntity<Long> getCount() {
        return ResponseEntity.ok(auditService.getTotalCount());
    }

    /**
     * Get storage size in bytes.
     * GET /api/audit/storage-size
     */
    @GetMapping("/storage-size")
    public ResponseEntity<Long> getStorageSize() {
        return ResponseEntity.ok(auditService.getStorageSize());
    }

    /**
     * Health check.
     * GET /api/audit/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
            "status", "UP",
            "totalRecords", auditService.getTotalCount(),
            "storageBytes", auditService.getStorageSize()
        ));
    }
}
