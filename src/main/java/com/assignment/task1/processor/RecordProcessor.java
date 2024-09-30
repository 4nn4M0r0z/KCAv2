package com.assignment.task1.processor;

import com.assignment.task1.deduplication.SlidingWindowDeduplication;
import com.assignment.task1.protobuf.LoginMessageV1;
import com.assignment.task1.protobuf.LoginMessageV2;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class RecordProcessor {

    private static final Logger logger = LoggerFactory.getLogger(RecordProcessor.class);

    private final SlidingWindowDeduplication deduplication;

    public RecordProcessor(SlidingWindowDeduplication deduplication) {
        this.deduplication = deduplication;
    }

    public void processRecord(String jsonString, Set<String> uniquePlayerLogins,
                              ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonString);

            if (jsonNode.has("country")) {
                // Parse as V2
                LoginMessageV2.Builder builderV2 = LoginMessageV2.newBuilder();
                JsonFormat.parser().merge(jsonString, builderV2);
                LoginMessageV2 messageV2 = builderV2.build();

                handleMessageV2(messageV2, uniquePlayerLogins, uniquePlayerLoginsByCountry);
            } else {
                // Parse as V1
                LoginMessageV1.Builder builderV1 = LoginMessageV1.newBuilder();
                JsonFormat.parser().merge(jsonString, builderV1);
                LoginMessageV1 messageV1 = builderV1.build();

                handleMessageV1(messageV1, uniquePlayerLogins);
            }
        } catch (Exception e) {
            logger.error("Failed to parse message: {}", e.getMessage());
        }
    }

    private void handleMessageV1(LoginMessageV1 message, Set<String> uniquePlayerLogins) {
        String playerId = message.getPlayerId();
        if (deduplication.isUniquePlayer(playerId)) {
            uniquePlayerLogins.add(playerId);
            logger.debug("Processed V1 unique player ID: {}", playerId);
        } else {
            logger.debug("Duplicate V1 player ID ignored: {}", playerId);
        }
    }

    private void handleMessageV2(LoginMessageV2 message, Set<String> uniquePlayerLogins,
                             ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry) {
        String playerId = message.getPlayerId();
        String country = message.getCountry();

        if (country == null || country.isEmpty()) {
            logger.warn("Received V2 message with empty country for player ID: {}", playerId);
            return;
        }

        if (deduplication.isUniquePlayer(playerId)) {
            uniquePlayerLogins.add(playerId);

            uniquePlayerLoginsByCountry.computeIfAbsent(country, k -> ConcurrentHashMap.newKeySet())
                    .add(playerId);

            logger.debug("Processed V2 unique player ID: {} from country: {}", playerId, country);
        } else {
            logger.debug("Duplicate V2 player ID ignored: {}", playerId);
        }
    }

}
