package com.pachure.audit.service;

import com.pachure.audit.model.AuditRecord;
import com.pachure.audit.repository.AuditRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Service layer for audit record operations.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class AuditService {

    private final AuditRepository auditRepository;

    /**
     * Initialize the audit storage.
     */
    public void initialize() {
        auditRepository.initializeTable();
    }

    /**
     * Create a new audit record.
     */
    public AuditRecord createAuditRecord(Map<String, Object> payload) {
        AuditRecord record = AuditRecord.builder()
                .id(UUID.randomUUID().toString())
                .timestamp(Instant.now())
                .payload(payload)
                .build();
        
        auditRepository.insert(record);
        log.debug("Created audit record: {}", record.getId());
        
        return record;
    }

    /**
     * Query audit records by timestamp range.
     */
    public List<AuditRecord> queryByTimeRange(Instant from, Instant to) {
        log.debug("Querying records from {} to {}", from, to);
        return auditRepository.findByTimestampBetween(from, to);
    }

    /**
     * Get total record count.
     */
    public long getTotalCount() {
        return auditRepository.count();
    }

    /**
     * Get current storage size in bytes.
     */
    public long getStorageSize() {
        return auditRepository.getStorageSize();
    }
}
