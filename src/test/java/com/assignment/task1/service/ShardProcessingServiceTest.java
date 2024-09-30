package com.assignment.task1.service;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.assignment.task1.config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class ShardProcessingServiceTest {

    @InjectMocks
    private ShardProcessingService shardProcessingService;

    @Mock
    private AmazonKinesis kinesisClient;

    @Mock
    private RecordProcessingService recordProcessingService;

    @Mock
    private AppConfig appConfig;

    @Mock
    private AppConfig.BufferConfig bufferConfig;

    @Mock
    private AppConfig.AwsConfig awsConfig;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(appConfig.getBuffer()).thenReturn(bufferConfig);
        when(bufferConfig.getSize()).thenReturn(1000);
        when(bufferConfig.getTimeMs()).thenReturn(5000);

        when(appConfig.getAws()).thenReturn(awsConfig);
        when(awsConfig.getStreamName()).thenReturn("test-stream");

        shardProcessingService.init();
    }

    @Test
    public void testProcessShard() throws Exception {
        Shard shard = new Shard().withShardId("shardId-000000000000");
        String shardIterator = "shardIterator";
        String nextShardIterator = "nextShardIterator";

        GetShardIteratorResult iteratorResult = new GetShardIteratorResult().withShardIterator(shardIterator);
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(iteratorResult);

        Record record = new Record().withData(ByteBuffer.wrap("testData".getBytes()));
        GetRecordsResult recordsResult = new GetRecordsResult()
                .withRecords(Collections.singletonList(record))
                .withNextShardIterator(nextShardIterator);

        // Return records on first call, empty list on second
        when(kinesisClient.getRecords(any(GetRecordsRequest.class)))
                .thenReturn(recordsResult)
                .thenReturn(new GetRecordsResult().withRecords(Collections.emptyList()).withNextShardIterator(null));

        CompletableFuture<Void> future = shardProcessingService.processShard(shard);
        future.get(); // Wait for processing to complete

        verify(kinesisClient, times(2)).getRecords(any(GetRecordsRequest.class));
        verify(recordProcessingService, times(1)).processRecord(anyString());
    }

    @Test
    public void testProcessShard_ShardIteratorNull() throws Exception {
        Shard shard = new Shard().withShardId("shardId-000000000001");
        when(kinesisClient.getShardIterator(any(GetShardIteratorRequest.class))).thenReturn(null);

        CompletableFuture<Void> future = shardProcessingService.processShard(shard);
        future.get(); // Wait for processing to complete

        // Check that getRecords is never called since shard iterator is null
        verify(kinesisClient, never()).getRecords(any(GetRecordsRequest.class));
        verify(recordProcessingService, never()).processRecord(anyString());
    }
}
