package com.assignment.task1.config;

import com.assignment.task1.deduplication.SlidingWindowDeduplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DeduplicationConfig {

    @Bean
    public SlidingWindowDeduplication slidingWindowDeduplication(AppConfig appConfig) {
        return new SlidingWindowDeduplication(appConfig.getDedup().getWindowMinutes());
    }
}
