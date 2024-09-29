package com.assignment.task1.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import lombok.Data;

@Component
@ConfigurationProperties(prefix = "app")
@Data
public class AppConfig {

    private AwsConfig aws;
    private BufferConfig buffer;
    private OutputConfig output;
    private DedupConfig dedup;
    private S3Config s3;

    @Data
    public static class AwsConfig {
        private String accessKey;
        private String secretKey;
        private String region;
        private String streamName;
    }

    @Data
    public static class BufferConfig {
        private int size;
        private int timeMs;
    }

    @Data
    public static class OutputConfig {
        private long frequencyMs;
        private boolean enableCompression;
    }

    @Data
    public static class DedupConfig {
        private long windowMinutes;
    }

    @Data
    public static class S3Config {
        private String bucketName;
    }
}
