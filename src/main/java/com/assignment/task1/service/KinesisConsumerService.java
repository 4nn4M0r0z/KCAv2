package com.assignment.task1.service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.assignment.task1.config.AppConfig;
import com.assignment.task1.dto.TotalUniquePlayerLogins;
import com.assignment.task1.dto.UniquePlayerLoginsByCountry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import org.springframework.scheduling.annotation.Scheduled;

import jakarta.annotation.PostConstruct;

@Service
public class KinesisConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KinesisConsumerService.class);

    private final AmazonKinesis kinesisClient;
    private final AppConfig appConfig;
    private RecordProcessingService recordProcessingService;
    private ShardProcessingService shardProcessingService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public KinesisConsumerService(AmazonKinesis kinesisClient,
                                  AppConfig appConfig,
                                  RecordProcessingService recordProcessingService,
                                  ShardProcessingService shardProcessingService) {
        this.kinesisClient = kinesisClient;
        this.appConfig = appConfig;
        this.recordProcessingService = recordProcessingService;
        this.shardProcessingService = shardProcessingService;
    }

    @PostConstruct
    public void init() {
        logger.info("Initializing KinesisConsumerService and starting consumption.");
        startConsuming();
    }

    public void startConsuming() {
        logger.info("Starting Kinesis Consumer Service");

        List<Shard> shards = getStreamShards();
        processShardsAsynchronously(shards);
    }

    private List<Shard> getStreamShards() {
        try {
            ListShardsRequest request = new ListShardsRequest()
                    .withStreamName(appConfig.getAws().getStreamName());
            ListShardsResult result = kinesisClient.listShards(request);
            List<Shard> shards = result.getShards();
            logger.info("Retrieved {} shard(s) from the stream", shards.size());
            return shards;
        } catch (Exception e) {
            logger.error("Failed to retrieve shards: {}", e.getMessage(), e);
            throw e;
        }
    }

    private void processShardsAsynchronously(List<Shard> shards) {
        logger.info("Processing shards: " + shards.size());
        for (Shard shard : shards) {
            shardProcessingService.processShard(shard);
        }
    }

    @Scheduled(fixedRateString = "${app.output.frequency-ms}")
    public void outputResults() {
        synchronized (this) {
                Set<String> uniquePlayerLogins = recordProcessingService.getUniquePlayerLogins();
                ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = recordProcessingService.getUniquePlayerLoginsByCountry();
                logger.debug("Total unique player logins: {}", uniquePlayerLogins.size());
                logger.debug("Unique player logins by country: {}", uniquePlayerLoginsByCountry);
    
                // Generate timestamp for file naming with milliseconds and UUID
                LocalDateTime currentDateTime = LocalDateTime.now();
                String dateString = currentDateTime.format(DateTimeFormatter.ISO_DATE);
                String hourString = String.format("%02d", currentDateTime.getHour());
                String minuteString = String.format("%02d", currentDateTime.getMinute());

                String timestamp = currentDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss_SSS")) + "_" + UUID.randomUUID().toString();

                logger.debug("Generated unique timestamp for filenames: {}", timestamp);

                // Define base output directory
                String baseOutputDir = "output";

                // Output directory structure
                String totalLoginsDir = String.format("%s/metric_name=TotalUniquePlayerLogins/date=%s/hour=%s", baseOutputDir, dateString, hourString);
                String loginsByCountryDir = String.format("%s/metric_name=UniquePlayerLoginsByCountry/date=%s/hour=%s", baseOutputDir, dateString, hourString);

                try {
                    // Create directories if they don't exist
                    Files.createDirectories(Paths.get(totalLoginsDir));
                    Files.createDirectories(Paths.get(loginsByCountryDir));
                } catch (IOException e) {
                    logger.error("Failed to create output directories: {}", e.getMessage(), e);
                    return;
                }

                // Prepare data for TotalUniquePlayerLogins DTO
                TotalUniquePlayerLogins totalLogins = new TotalUniquePlayerLogins();
                totalLogins.setDate(dateString);
                totalLogins.setHour(hourString);
                totalLogins.setMinute(minuteString);
                totalLogins.setMetricName("TotalUniquePlayerLogins");
                totalLogins.setLoginCount(uniquePlayerLogins.size());

                // Log and invoke writeJsonToFile for TotalUniquePlayerLogins
                String totalLoginsFilename = String.format("%s/total_unique_player_logins_%s.json", totalLoginsDir, timestamp);
                logger.debug("Invoking writeJsonToFile for TotalUniquePlayerLogins with filename '{}'", totalLoginsFilename);
                writeJsonToFile(totalLoginsFilename, totalLogins, false);

                // Prepare data for UniquePlayerLoginsByCountry DTO
                List<UniquePlayerLoginsByCountry> loginsByCountryList = new ArrayList<>();
                for (Map.Entry<String, Set<String>> entry : uniquePlayerLoginsByCountry.entrySet()) {
                    String country = entry.getKey();
                    int count = entry.getValue().size();

                    // missing or unknown country
                    if (country == null || country.isEmpty()) {
                        country = "N/A";
                    }

                    UniquePlayerLoginsByCountry countryData = new UniquePlayerLoginsByCountry();
                    countryData.setDate(dateString);
                    countryData.setHour(hourString);
                    countryData.setMinute(minuteString);
                    countryData.setMetricName("UniquePlayerLoginsByCountry");
                    countryData.setCountry(country);
                    countryData.setLoginCount(count);

                    loginsByCountryList.add(countryData);
                }

                // Log and invoke writeJsonToFile for UniquePlayerLoginsByCountry
                logger.debug("Preparing to write {} unique_player_logins_by_country entries.", loginsByCountryList.size());
                for (UniquePlayerLoginsByCountry dto : loginsByCountryList) {
                    logger.debug("Country: {}, Login Count: {}", dto.getCountry(), dto.getLoginCount());
                }

                String loginsByCountryFilename = String.format("%s/unique_player_logins_by_country_%s.json", loginsByCountryDir, timestamp);
                logger.debug("Invoking writeJsonToFile for UniquePlayerLoginsByCountry with filename '{}'", loginsByCountryFilename);
                writeJsonToFile(loginsByCountryFilename, loginsByCountryList, true);

                logger.info("Aggregated results written to output files.");

                // Clear aggregated results for the next interval
                uniquePlayerLogins.clear();
                uniquePlayerLoginsByCountry.clear();
                logger.debug("Clearing aggregated data...");
                recordProcessingService.clearAggregations();
                logger.debug("Aggregated data cleared.");
            }
    }
    
    private void writeJsonToFile(String filename, Object data, boolean isList) {
        try {
            ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
            String jsonContent;

            if (isList && data instanceof List) {
                jsonContent = writer.writeValueAsString(data);
            } else {
                jsonContent = writer.writeValueAsString(data);
            }

            Files.write(Paths.get(filename), jsonContent.getBytes(StandardCharsets.UTF_8));
            logger.debug("Successfully wrote JSON data to file: {}", filename);
        } catch (IOException e) {
            logger.error("Failed to write JSON data to file '{}': {}", filename, e.getMessage(), e);
        }
    }

}
