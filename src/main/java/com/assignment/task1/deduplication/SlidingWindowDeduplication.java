package com.assignment.task1.deduplication;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.TimeUnit;

/**
 * Deduplication mechanism using Caffeine Cache for sliding window deduplication.
 */
public class SlidingWindowDeduplication {

    private Cache<String, Boolean> seenIds;

    /**
     * Initializes the deduplication cache with the specified window size.
     *
     * @param windowSizeInMinutes The size of the sliding window in minutes.
     */
    public SlidingWindowDeduplication(long windowSizeInMinutes) {
        this.seenIds = Caffeine.newBuilder()
                .expireAfterWrite(windowSizeInMinutes, TimeUnit.MINUTES)
                .maximumSize(100000) // modify based on expected unique IDs
                .build();
    }

    /**
     * Checks if the player ID is unique within the deduplication window.
     *
     * @param playerId The player ID to check.
     * @return true if the player ID is unique; false otherwise.
     */
    public boolean isUniquePlayer(String playerId) {
        return seenIds.asMap().putIfAbsent(playerId, Boolean.TRUE) == null;
    }

    /**
     * Clears the deduplication cache.
     */
    public void shutdown() {
        seenIds.invalidateAll();
    }
}
