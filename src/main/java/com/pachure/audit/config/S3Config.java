package com.pachure.audit.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.net.URI;

/**
 * S3/MinIO configuration for audit storage.
 */
@Configuration
public class S3Config {

    @Value("${audit.s3.endpoint:http://localhost:9000}")
    private String endpoint;

    @Value("${audit.s3.access-key:minioadmin}")
    private String accessKey;

    @Value("${audit.s3.secret-key:minioadmin}")
    private String secretKey;

    @Value("${audit.s3.bucket:audit-bucket}")
    private String bucket;

    @Value("${audit.s3.region:us-east-1}")
    private String region;

    @Bean
    public S3Client s3Client() {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .forcePathStyle(true) // Required for MinIO
                .build();
    }

    @Bean
    public String auditBucket() {
        return bucket;
    }
}
