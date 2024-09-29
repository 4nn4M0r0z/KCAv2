package com.assignment.task1.service;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.assignment.task1.config.AppConfig;

import jakarta.annotation.PostConstruct;

import java.util.ArrayList;
import java.util.List;
import java.nio.charset.StandardCharsets;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;

@Service
public class ShardProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ShardProcessingService.class);

    private static final int MAX_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 2000;

    private int bufferSize;
    private int bufferTimeMs;

    @Autowired
    private AppConfig appConfig;

    @Autowired
    private RecordProcessingService recordProcessingService;

    @Autowired
    private AmazonKinesis kinesisClient;

    @PostConstruct
    public void init() {
        this.bufferSize = appConfig.getBuffer().getSize();
        this.bufferTimeMs = appConfig.getBuffer().getTimeMs();
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> processShard(Shard shard) {
        String shardId = shard.getShardId();
        int retryCount = 0;

        while (retryCount <= MAX_RETRIES) {
            try {
                logger.info("Processing shard '{}', attempt {}/{}", shardId, retryCount + 1, MAX_RETRIES + 1);

                String shardIterator = getShardIterator(shard);
                if (shardIterator == null) {
                    logger.warn("Shard iterator is null for shard '{}'. Skipping shard.", shardId);
                    return CompletableFuture.completedFuture(null);
                }

                List<Record> buffer = new ArrayList<>();
                long lastBufferTime = System.currentTimeMillis();

                while (shardIterator != null) {
                    try {
                        GetRecordsRequest recordsRequest = new GetRecordsRequest()
                                .withShardIterator(shardIterator)
                                .withLimit(1000);
                        GetRecordsResult recordsResult = kinesisClient.getRecords(recordsRequest);
                        List<Record> records = recordsResult.getRecords();

                        logger.info("Fetched {} records from shard '{}'", records.size(), shardId);

                        if (!records.isEmpty()) {
                            buffer.addAll(records);
                        }

                        shardIterator = recordsResult.getNextShardIterator();

                        // Check if buffer conditions are met
                        if (buffer.size() >= bufferSize || (System.currentTimeMillis() - lastBufferTime) >= bufferTimeMs) {
                            logger.debug("Processing {} records from shard '{}'", buffer.size(), shardId);
                            processRecords(buffer);
                            buffer.clear();
                            lastBufferTime = System.currentTimeMillis();
                        }

                        // Sleep if no records are fetched to prevent tight looping
                        if (records.isEmpty()) {
                            Thread.sleep(1000);
                        }

                    } catch (InterruptedException e) {
                        logger.error("Shard processing thread interrupted for shard '{}': {}", shardId, e.getMessage(), e);
                        Thread.currentThread().interrupt();
                        throw e; //to outer catch block
                    } catch (Exception e) {
                        logger.error("Error fetching records for shard '{}': {}", shardId, e.getMessage(), e);
                        throw e; // to outer catch block
                    }
                }

                // Process any remaining records in the buffer
                if (!buffer.isEmpty()) {
                    logger.info("Processing remaining {} records from shard '{}'", buffer.size(), shardId);
                    processRecords(buffer);
                    buffer.clear();
                }

                logger.info("Completed processing shard '{}'", shardId);
                return CompletableFuture.completedFuture(null);
            } catch (InterruptedException e) {
                logger.error("Shard processing interrupted for shard '{}': {}", shardId, e.getMessage(), e);
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                retryCount++;
                if (retryCount > MAX_RETRIES) {
                    logger.error("Max retries reached for shard '{}'. Skipping shard.", shardId);
                    break;
                } else {
                    logger.warn("Error processing shard '{}'. Retrying attempt {}/{} after delay.", shardId, retryCount, MAX_RETRIES, e);
                    try {
                        Thread.sleep(RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        logger.error("Retry sleep interrupted for shard '{}': {}", shardId, ie.getMessage(), ie);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private String getShardIterator(Shard shard) {
        try {
            GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest()
                    .withStreamName(appConfig.getAws().getStreamName())
                    .withShardId(shard.getShardId())
                    .withShardIteratorType(ShardIteratorType.TRIM_HORIZON);
            GetShardIteratorResult iteratorResult = kinesisClient.getShardIterator(iteratorRequest);
            return iteratorResult.getShardIterator();
        } catch (ResourceNotFoundException e) {
            logger.error("Stream or shard not found for shard '{}': {}", shard.getShardId(), e.getMessage(), e);
            return null;
        } catch (InvalidArgumentException e) {
            logger.error("Invalid argument for shard '{}': {}", shard.getShardId(), e.getMessage(), e);
            return null;
        } catch (Exception e) {
            logger.error("Failed to get shard iterator for shard '{}': {}", shard.getShardId(), e.getMessage(), e);
            return null;
        }
    }

    private void processRecords(List<Record> records) {
        for (Record record : records) {
            logger.debug("Processing {} records", records.size());
            String jsonString = new String(record.getData().array(), StandardCharsets.UTF_8);
            recordProcessingService.processRecord(jsonString);
        }
    }
}