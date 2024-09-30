package com.assignment.task1.service;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import com.assignment.task1.config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.util.Arrays;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class KinesisConsumerServiceTest {

    @InjectMocks
    private KinesisConsumerService kinesisConsumerService;

    @Mock
    private AmazonKinesis kinesisClient;

    @Mock
    private AppConfig appConfig;

    @Mock
    private AppConfig.AwsConfig awsConfig;

    @Mock
    private ShardProcessingService shardProcessingService;

    // Declare shards as instance variables
    private Shard shard1;
    private Shard shard2;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(appConfig.getAws()).thenReturn(awsConfig);
        when(awsConfig.getStreamName()).thenReturn("test-stream");
        shard1 = new Shard().withShardId("shardId-000000000000");
        shard2 = new Shard().withShardId("shardId-000000000001");
        ListShardsResult listShardsResult = new ListShardsResult().withShards(Arrays.asList(shard1, shard2));
        when(kinesisClient.listShards(any(ListShardsRequest.class))).thenReturn(listShardsResult);
        kinesisConsumerService.init();
    }

    @Test
    public void testStartConsuming() {
        // Verify that processShard is called for evary shard
        verify(shardProcessingService).processShard(shard1);
        verify(shardProcessingService).processShard(shard2);

        // Verify that processShard is called exactly 2 times
        verify(shardProcessingService, times(2)).processShard(any(Shard.class));

        verifyNoMoreInteractions(shardProcessingService);
    }
}
