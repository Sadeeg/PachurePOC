package com.pachure.audit.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;

/**
 * DuckDB configuration for Apache Parquet storage.
 * 
 * Uses DuckDB in embedded mode - no external database server needed.
 * Data is stored in Parquet files for optimal compression and query performance.
 */
@Configuration
public class DuckDbConfig {

    @Value("${audit.storage.path:./data/audit}")
    private String storagePath;

    @Bean
    public DataSource dataSource() {
        // Create directory if not exists
        java.nio.file.Path path = java.nio.file.Path.of(storagePath);
        try {
            java.nio.file.Files.createDirectories(path);
        } catch (Exception e) {
            // Ignore - will fail on write anyway
        }

        return DataSourceBuilder.create()
                .driverClassName("org.duckdb.DuckDBDriver")
                .url("jdbc:duckdb:" + storagePath + "/audit.db")
                .build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
