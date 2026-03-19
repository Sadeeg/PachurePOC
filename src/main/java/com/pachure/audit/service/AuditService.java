package com.pachure.audit.service;

import com.pachure.audit.model.AuditRecord;
import com.pachure.audit.repository.S3AuditRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Service for audit record operations.
 * Uses S3 repository for storage.
 */
@Service
public class AuditService {

    private static final Logger log = LoggerFactory.getLogger(AuditService.class);

    private final S3AuditRepository repository;

    @Autowired
    public AuditService(S3AuditRepository repository) {
        this.repository = repository;
    }

    /**
     * Initialize the audit storage.
     */
    public void initialize() {
        log.info("Initializing S3 audit storage");
        // S3 bucket is created automatically
    }

    /**
     * Save a single audit record.
     */
    public AuditRecord save(AuditRecord record) {
        if (record.getId() == null) {
            record.setId(java.util.UUID.randomUUID().toString());
        }
        if (record.getTimestamp() == null) {
            record.setTimestamp(Instant.now());
        }
        repository.save(record);
        return record;
    }

    /**
     * Save multiple audit records.
     */
    public void batchSave(List<AuditRecord> records) {
        repository.batchSave(records);
    }

    /**
     * Find records by timestamp range.
     */
    public List<AuditRecord> findByTimestampBetween(Instant from, Instant to) {
        return repository.findByTimestampBetween(from, to);
    }

    /**
     * Count all records.
     */
    public long count() {
        return repository.count();
    }

    /**
     * Get storage size in bytes.
     */
    public long getStorageSize() {
        return repository.getStorageSize();
    }

    /**
     * Find a single record by ID.
     */
    public Optional<AuditRecord> findById(String id) {
        // S3 implementation - would need list + filter
        return Optional.empty();
    }
}
