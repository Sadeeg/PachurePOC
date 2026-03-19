package com.pachure.audit.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jdbc.JdbcRepositoriesAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.boot.jdbc.DataSourceBuilder;

import javax.sql.DataSource;

/**
 * DuckDB configuration for Apache Parquet storage.
 */
@Configuration
@EnableAutoConfiguration(exclude = {JdbcRepositoriesAutoConfiguration.class})
public class DuckDbConfig {

    @Value("${audit.storage.path:./data/audit}")
    private String storagePath;

    @Bean
    public DataSource dataSource() {
        java.nio.file.Path path = java.nio.file.Path.of(storagePath);
        try {
            java.nio.file.Files.createDirectories(path);
        } catch (Exception e) {
            // Ignore
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
