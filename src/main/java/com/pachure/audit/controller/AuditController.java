package com.pachure.audit.controller;

import com.pachure.audit.model.AuditRecord;
import com.pachure.audit.service.AuditService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * REST controller for audit operations.
 */
@RestController
@RequestMapping("/api/audit")
public class AuditController {

    private final AuditService auditService;

    @Autowired
    public AuditController(AuditService auditService) {
        this.auditService = auditService;
    }

    /**
     * Create a new audit record.
     */
    @PostMapping
    public ResponseEntity<AuditRecord> createAuditRecord(@RequestBody Map<String, Object> payload) {
        AuditRecord record = AuditRecord.builder()
                .timestamp(Instant.now())
                .payload(payload)
                .build();
        AuditRecord saved = auditService.save(record);
        return ResponseEntity.ok(saved);
    }

    /**
     * Query audit records by timestamp range.
     */
    @GetMapping
    public ResponseEntity<List<AuditRecord>> queryByTimeRange(
            @RequestParam Instant from,
            @RequestParam Instant to) {
        List<AuditRecord> records = auditService.findByTimestampBetween(from, to);
        return ResponseEntity.ok(records);
    }

    /**
     * Get total record count.
     */
    @GetMapping("/count")
    public ResponseEntity<Long> getTotalCount() {
        return ResponseEntity.ok(auditService.count());
    }

    /**
     * Get storage statistics.
     */
    @GetMapping("/storage")
    public ResponseEntity<Map<String, Object>> getStorageInfo() {
        long count = auditService.count();
        long size = auditService.getStorageSize();
        
        return ResponseEntity.ok(Map.of(
                "recordCount", count,
                "storageBytes", size,
                "avgBytesPerRecord", count > 0 ? size / count : 0
        ));
    }
}
