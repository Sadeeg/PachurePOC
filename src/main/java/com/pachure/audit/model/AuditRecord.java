package com.pachure.audit.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Audit record entity for storing audit logs in Apache Parquet format.
 * 
 * Schema:
 * - id: UUID string
 * - timestamp: ISO-8601 instant
 * - payload: JSON object (stored as string in Parquet)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditRecord {

    private String id;
    private Instant timestamp;
    private Map<String, Object> payload;

    /**
     * Serialize payload to JSON string for Parquet storage.
     */
    public String getPayloadAsJson() {
        if (payload == null) {
            return "{}";
        }
        try {
            return new ObjectMapper().writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            return "{}";
        }
    }

    /**
     * Deserialize payload from JSON string.
     */
    public void setPayloadFromJson(String json) {
        if (json == null || json.isEmpty()) {
            this.payload = Map.of();
            return;
        }
        try {
            this.payload = new ObjectMapper().readValue(json, 
                new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            this.payload = Map.of();
        }
    }
}
