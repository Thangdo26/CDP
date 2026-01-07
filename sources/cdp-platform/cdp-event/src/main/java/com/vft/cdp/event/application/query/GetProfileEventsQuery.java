package com.vft.cdp.event.application.query;

import java.time.Instant;

/**
 * Query object for getting profile events
 *
 * WHY QUERY OBJECT?
 * - Type safety: Compile-time validation
 * - Validation: Business rules in one place
 * - Clarity: Intent clear from object name
 *
 * ALTERNATIVE (BAD):
 * getEvents(String t, String p, int pg, int sz, Instant s, Instant e)
 * → Too many parameters, unclear meaning
 *
 * WITH QUERY OBJECT (GOOD):
 * getEvents(GetProfileEventsQuery query)
 * → Clear intent, easy to extend
 */
public record GetProfileEventsQuery(
        String tenantId,
        String profileId,
        int page,
        int size,
        Instant startTime,
        Instant endTime
) {
    /**
     * COMPACT CONSTRUCTOR - Record feature for validation
     *
     * Automatically called after field initialization
     */
    public GetProfileEventsQuery {
        // Validation
        if (tenantId == null || tenantId.isBlank()) {
            throw new IllegalArgumentException("Tenant ID is required");
        }
        if (profileId == null || profileId.isBlank()) {
            throw new IllegalArgumentException("Profile ID is required");
        }
        if (page < 0) {
            throw new IllegalArgumentException("Page must be >= 0");
        }
        if (size < 1 || size > 1000) {
            throw new IllegalArgumentException("Size must be between 1 and 1000");
        }
        if (startTime != null && endTime != null) {
            if (startTime.isAfter(endTime)) {
                throw new IllegalArgumentException("Start time must be before end time");
            }
        }
    }

    /**
     * FACTORY METHOD with defaults
     */
    public static GetProfileEventsQuery create(
            String tenantId,
            String profileId,
            int page,
            int size
    ) {
        return new GetProfileEventsQuery(
                tenantId,
                profileId,
                page,
                size,
                null,  // No time filter
                null
        );
    }

    /**
     * Check if time range filter is applied
     */
    public boolean hasTimeRange() {
        return startTime != null && endTime != null;
    }
}