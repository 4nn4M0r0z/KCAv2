package com.assignment.task1.deduplication;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SlidingWindowDeduplicationTest {

    private SlidingWindowDeduplication deduplication;
    private final long windowSizeMs = 1000; // 1 second window

    @BeforeEach
    public void setUp() {
        deduplication = new SlidingWindowDeduplication(windowSizeMs);
    }

    @Test
    public void testIsUniquePlayer_FirstOccurrence() {
        String playerId = "player1";
        assertTrue(deduplication.isUniquePlayer(playerId), "First occurrence should be unique");
    }

    @Test
    public void testIsUniquePlayer_DuplicateWithinWindow() {
        String playerId = "player1";
        deduplication.isUniquePlayer(playerId); // First occurrence
        assertFalse(deduplication.isUniquePlayer(playerId), "Duplicate within window should not be unique");
    }

    @Test
    void testIsUniquePlayer() {
        String playerId1 = "player1";
        assertTrue(deduplication.isUniquePlayer(playerId1), "Player ID should be unique on first occurrence");

        assertFalse(deduplication.isUniquePlayer(playerId1), "Player ID should not be unique on second occurrence");

        String playerId2 = "player2";
        assertTrue(deduplication.isUniquePlayer(playerId2), "Player ID should be unique on first occurrence for a different player");
    }

    @Test
    void testShutdown() {
        String playerId1 = "player1";
        assertTrue(deduplication.isUniquePlayer(playerId1), "Player ID should be unique on first occurrence");
        // Clear the cache and check that the same player ID is now considered unique again
        deduplication.shutdown();
        assertTrue(deduplication.isUniquePlayer(playerId1), "Player ID should be unique after cache is cleared");
    }

    @Test
    void testHandlingHighVolumeOfUniqueIds() {
        // Test with a large number of unique player IDs
        int numPlayers = 100000;
        for (int i = 0; i < numPlayers; i++) {
            String playerId = "player" + i;
            assertTrue(deduplication.isUniquePlayer(playerId), "Player ID should be unique for player " + i);
        }

        // Test that a previously added ID is not unique
        assertFalse(deduplication.isUniquePlayer("player50000"), "Player ID should not be unique on second occurrence");
    }

    @Test
    void testMixedUniqueAndNonUniqueIds() {
        // Add two unique player IDs
        String playerId1 = "player1";
        String playerId2 = "player2";

        assertTrue(deduplication.isUniquePlayer(playerId1), "Player ID 1 should be unique on first occurrence");
        assertTrue(deduplication.isUniquePlayer(playerId2), "Player ID 2 should be unique on first occurrence");

        // Add again the first player ID (non-unique)
        assertFalse(deduplication.isUniquePlayer(playerId1), "Player ID 1 should not be unique on second occurrence");

        // Add a new player ID (unique)
        String playerId3 = "player3";
        assertTrue(deduplication.isUniquePlayer(playerId3), "Player ID 3 should be unique on first occurrence");

        // Add again the second player ID (non-unique)
        assertFalse(deduplication.isUniquePlayer(playerId2), "Player ID 2 should not be unique on second occurrence");
    }
}