package com.vft.cdp.event.api.response;

import com.vft.cdp.event.application.dto.EventDto;

import java.time.Instant;
import java.util.Map;

/**
 * API Response for single event
 *
 * WHY SEPARATE FROM EventDto?
 * - API Layer concerns (JSON formatting, API versioning)
 * - Application Layer DTO is internal
 * - Can add API-specific fields (e.g., _links for HATEOAS)
 *
 * RECORD vs CLASS?
 * - Record: Immutable, auto-generated methods
 * - Perfect for DTOs/Responses
 */
public record EventResponse(
        String id,
        String tenantId,
        String appId,
        String profileId,
        String eventName,
        Instant eventTime,
        Instant normalizedTime,
        Map<String, Object> properties,
        Map<String, Object> traits,
        Map<String, Object> context
) {
    /**
     * Factory method: EventDto → EventResponse
     *
     * WHY?
     * - Conversion logic centralized
     * - Easy to add transformations
     * - Consistent pattern across all responses
     */
    public static EventResponse fromDto(EventDto dto) {
        return new EventResponse(
                dto.id(),
                dto.tenantId(),
                dto.appId(),
                dto.profileId(),
                dto.eventName(),
                dto.eventTime(),
                dto.normalizedTime(),
                dto.properties(),
                dto.traits(),
                dto.context()
        );
    }
}