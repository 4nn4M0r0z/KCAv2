package com.assignment.task1.processor;

import com.assignment.task1.deduplication.SlidingWindowDeduplication;
import com.assignment.task1.protobuf.LoginMessageV1;
import com.assignment.task1.protobuf.LoginMessageV2;
import com.google.protobuf.InvalidProtocolBufferException;
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
            // Attempt to parse with V2 schema
            LoginMessageV2.Builder builderV2 = LoginMessageV2.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builderV2);
            LoginMessageV2 messageV2 = builderV2.build();

            handleMessageV2(messageV2, uniquePlayerLogins, uniquePlayerLoginsByCountry);

        } catch (InvalidProtocolBufferException e1) {
            try {
                // Fallback to V1 schema
                LoginMessageV1.Builder builderV1 = LoginMessageV1.newBuilder();
                JsonFormat.parser().ignoringUnknownFields().merge(jsonString, builderV1);
                LoginMessageV1 messageV1 = builderV1.build();

                handleMessageV1(messageV1, uniquePlayerLogins);

            } catch (InvalidProtocolBufferException e2) {
                logger.error("Failed to parse message: {}", e2.getMessage());
            }
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

        if (deduplication.isUniquePlayer(playerId)) {
            uniquePlayerLogins.add(playerId);

            // Update country-specific logins
            uniquePlayerLoginsByCountry.computeIfAbsent(country, k -> ConcurrentHashMap.newKeySet())
                    .add(playerId);

            logger.debug("Processed V2 unique player ID: {} from country: {}", playerId, country);
        } else {
            logger.debug("Duplicate V2 player ID ignored: {}", playerId);
        }
    }
}
