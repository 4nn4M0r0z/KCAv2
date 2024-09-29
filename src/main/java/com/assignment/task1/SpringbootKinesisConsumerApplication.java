package com.assignment.task1;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SpringBootApplication
@EnableScheduling
@EnableAsync
public class SpringbootKinesisConsumerApplication {

    private static final Logger logger = LoggerFactory.getLogger(SpringbootKinesisConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SpringbootKinesisConsumerApplication.class, args);
    }

    @PostConstruct
    public void init() {
        logger.info("Spring Boot application has started successfully.");
    }
}
