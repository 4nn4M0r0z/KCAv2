package com.assignment.task1.service;

import com.assignment.task1.deduplication.SlidingWindowDeduplication;
import com.assignment.task1.protobuf.LoginMessageV1;
import com.assignment.task1.protobuf.LoginMessageV2;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.Set;

@Service
public class RecordProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(RecordProcessingService.class);

    private final SlidingWindowDeduplication deduplication;

    private final Set<String> uniquePlayerLogins = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = new ConcurrentHashMap<>();

    public RecordProcessingService(SlidingWindowDeduplication deduplication) {
        this.deduplication = deduplication;
    }

    public void processRecord(String jsonString) {
        try {
            logger.debug("Processing record: {}", jsonString);
            // Attempting to parse with V2 schema
            LoginMessageV2.Builder builderV2 = LoginMessageV2.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builderV2);
            LoginMessageV2 messageV2 = builderV2.build();

            handleMessageV2(messageV2);

        } catch (InvalidProtocolBufferException e1) {
            try {
                // Fallback to V1 schema
                LoginMessageV1.Builder builderV1 = LoginMessageV1.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builderV1);
                LoginMessageV1 messageV1 = builderV1.build();

                handleMessageV1(messageV1);

            } catch (InvalidProtocolBufferException e2) {
                logger.error("Failed to parse message: {}", e2.getMessage());
            }
        }
    }

    private void handleMessageV1(LoginMessageV1 message) {
        String playerId = message.getPlayerId();
        logger.debug("Processing V1 message for player ID: {}", playerId);
        if (deduplication.isUniquePlayer(playerId)) {
            uniquePlayerLogins.add(playerId);
            logger.debug("Added player ID to unique logins: {}. Total unique logins: {}", playerId, uniquePlayerLogins.size());
        } else {
            logger.debug("Duplicate V1 player ID ignored: {}", playerId);
        }
    }

    private void handleMessageV2(LoginMessageV2 message) {
        String playerId = message.getPlayerId();
        String country = message.getCountry();
        logger.debug("Processing V2 message for player ID: {}, country: {}", playerId, country);
        if (deduplication.isUniquePlayer(playerId)) {
            uniquePlayerLogins.add(playerId);
            uniquePlayerLoginsByCountry.computeIfAbsent(country, k -> ConcurrentHashMap.newKeySet())
                    .add(playerId);
            logger.debug("Added player ID to unique logins: {}. Total unique logins: {}", playerId, uniquePlayerLogins.size());
            logger.debug("Added player ID: {} to country: {}. Total logins for country: {}", playerId, country, uniquePlayerLoginsByCountry.get(country).size());
        } else {
            logger.debug("Duplicate V2 player ID ignored: {}", playerId);
        }
    }

    public Set<String> getUniquePlayerLogins() {
        return uniquePlayerLogins;
    }

    public ConcurrentMap<String, Set<String>> getUniquePlayerLoginsByCountry() {
        return uniquePlayerLoginsByCountry;
    }

    public void clearAggregations() {
        uniquePlayerLogins.clear();
        uniquePlayerLoginsByCountry.clear();
    }
}
