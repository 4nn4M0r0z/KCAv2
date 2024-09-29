package com.assignment.task1.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AwsClientConfig {

    @Autowired
    private AppConfig appConfig;

    @Bean
    public AmazonKinesis amazonKinesis() {
        BasicAWSCredentials awsCreds = new BasicAWSCredentials(
                appConfig.getAws().getAccessKey(),
                appConfig.getAws().getSecretKey()
        );

        return AmazonKinesisClientBuilder.standard()
                .withRegion(appConfig.getAws().getRegion())
                .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
                .build();
    }
}
