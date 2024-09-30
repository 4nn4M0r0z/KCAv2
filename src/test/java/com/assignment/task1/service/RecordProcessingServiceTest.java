package com.assignment.task1.service;

import com.assignment.task1.deduplication.SlidingWindowDeduplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

public class RecordProcessingServiceTest {

    private RecordProcessingService recordProcessingService;
    private SlidingWindowDeduplication deduplication;

    @BeforeEach
    public void setUp() {
        deduplication = Mockito.mock(SlidingWindowDeduplication.class);
        recordProcessingService = new RecordProcessingService(deduplication);
    }

    @Test
    public void testProcessRecord_LoginMessageV1_Unique() throws Exception {
        String jsonString = "{\"playerId\":\"player1\"}";

        when(deduplication.isUniquePlayer("player1")).thenReturn(true);

        recordProcessingService.processRecord(jsonString);

        Set<String> uniquePlayerLogins = recordProcessingService.getUniquePlayerLogins();
        ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = recordProcessingService.getUniquePlayerLoginsByCountry();

        assertTrue(uniquePlayerLogins.contains("player1"), "Unique player logins should contain 'player1'");
        assertFalse(uniquePlayerLoginsByCountry.isEmpty(), "uniquePlayerLoginsByCountry should not be empty for V1 messages");
    }

    @Test
    public void testProcessRecord_LoginMessageV2_Unique() throws Exception {
        String jsonString = "{\"playerId\":\"player2\",\"country\":\"US\"}";

        when(deduplication.isUniquePlayer("player2")).thenReturn(true);

        recordProcessingService.processRecord(jsonString);

        Set<String> uniquePlayerLogins = recordProcessingService.getUniquePlayerLogins();
        ConcurrentMap<String, Set<String>> uniquePlayerLoginsByCountry = recordProcessingService.getUniquePlayerLoginsByCountry();

        assertTrue(uniquePlayerLogins.contains("player2"), "Unique player logins should contain 'player2'");
        assertTrue(uniquePlayerLoginsByCountry.containsKey("US"), "uniquePlayerLoginsByCountry should contain 'US'");
        assertTrue(uniquePlayerLoginsByCountry.get("US").contains("player2"), "Country 'US' should contain 'player2'");
    }
}
