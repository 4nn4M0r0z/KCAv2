package com.assignment.task1.processor;

import com.assignment.task1.deduplication.SlidingWindowDeduplication;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.junit.jupiter.api.BeforeEach;

public class RecordProcessorTest {

    private RecordProcessor recordProcessor;
    private SlidingWindowDeduplication deduplication;

    @BeforeEach
    public void setUp() {
        deduplication = mock(SlidingWindowDeduplication.class);
        recordProcessor = new RecordProcessor(deduplication);
    }

    @Test
    public void testProcessRecord_LoginMessageV1_Unique() throws Exception {
        String jsonString = "{\"playerId\":\"player1\"}";
        Set<String> uniquePlayerLogins = ConcurrentHashMap.newKeySet();
        ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = new ConcurrentHashMap<>();

        when(deduplication.isUniquePlayer("player1")).thenReturn(true);

        recordProcessor.processRecord(jsonString, uniquePlayerLogins, uniquePlayerLoginsByCountry);

        assertTrue(uniquePlayerLogins.contains("player1"), "Unique player logins should contain 'player1'");
        assertTrue(uniquePlayerLoginsByCountry.isEmpty(), "uniquePlayerLoginsByCountry should be empty for V1 messages");
    }

    @Test
    public void testProcessRecord_LoginMessageV2_Unique() throws Exception {
        String jsonString = "{\"playerId\":\"player2\",\"country\":\"US\"}";
        Set<String> uniquePlayerLogins = ConcurrentHashMap.newKeySet();
        ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = new ConcurrentHashMap<>();

        when(deduplication.isUniquePlayer("player2")).thenReturn(true);

        recordProcessor.processRecord(jsonString, uniquePlayerLogins, uniquePlayerLoginsByCountry);

        assertTrue(uniquePlayerLogins.contains("player2"), "Unique player logins should contain 'player2'");
        assertTrue(uniquePlayerLoginsByCountry.containsKey("US"), "uniquePlayerLoginsByCountry should contain 'US'");
        assertTrue(uniquePlayerLoginsByCountry.get("US").contains("player2"), "Country 'US' should contain 'player2'");
    }

    @Test
    public void testProcessRecord_Duplicate() throws Exception {
        String jsonString = "{\"playerId\":\"player1\"}";
        Set<String> uniquePlayerLogins = ConcurrentHashMap.newKeySet();
        ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = new ConcurrentHashMap<>();

        when(deduplication.isUniquePlayer("player1")).thenReturn(false);

        recordProcessor.processRecord(jsonString, uniquePlayerLogins, uniquePlayerLoginsByCountry);

        assertFalse(uniquePlayerLogins.contains("player1"), "Duplicate player ID should not be added");
        assertTrue(uniquePlayerLoginsByCountry.isEmpty(), "uniquePlayerLoginsByCountry should remain empty");
    }

    @Test
    public void testProcessRecord_InvalidMessage() {
        String jsonString = "{\"invalidField\":\"invalidValue\"}";
        Set<String> uniquePlayerLogins = ConcurrentHashMap.newKeySet();
        ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = new ConcurrentHashMap<>();

        recordProcessor.processRecord(jsonString, uniquePlayerLogins, uniquePlayerLoginsByCountry);

        // Since the message is invalid, no entries should be added
        assertTrue(uniquePlayerLogins.isEmpty(), "uniquePlayerLogins should be empty for invalid messages");
        assertTrue(uniquePlayerLoginsByCountry.isEmpty(), "uniquePlayerLoginsByCountry should be empty for invalid messages");
    }

}
