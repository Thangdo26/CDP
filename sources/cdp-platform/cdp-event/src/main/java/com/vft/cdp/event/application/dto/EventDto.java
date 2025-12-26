package com.vft.cdp.event.application.dto;

import com.vft.cdp.event.domain.model.Event;
import com.vft.cdp.event.infra.es.EventDocument;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Data Transfer Object for Event
 *
 * WHY RECORD?
 * - Immutable by default (all fields final)
 * - Auto-generated: constructor, getters, equals, hashCode, toString
 * - Concise: 1 line vs 50+ lines boilerplate
 *
 * RECORD vs CLASS:
 * record EventDto(String id) {}
 * ≈
 * public final class EventDto {
 *     private final String id;
 *     public EventDto(String id) { this.id = id; }
 *     public String id() { return id; }
 *     public boolean equals(Object o) {...}
 *     public int hashCode() {...}
 *     public String toString() {...}
 * }
 */
public record EventDto(
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
     * FACTORY METHOD: Domain → DTO
     *
     * WHY STATIC FACTORY?
     * - Encapsulation: Conversion logic lives with DTO
     * - Convenience: EventDto.fromDomain(event) is clear
     * - Consistency: Same pattern across all DTOs
     */
    public static EventDto fromDomain(Event event) {
        return new EventDto(
                event.getId(),
                event.getTenantId(),
                event.getAppId(),
                event.getProfileId(),
                event.getEventName(),
                event.getEventTime(),
                event.getNormalizedTime(),
                event.getProperties(),
                event.getTraits(),
                event.getContext()
        );
    }

    /**
     * FACTORY METHOD: Infrastructure (ES Document) → DTO
     *
     * WHY SEPARATE METHOD?
     * - Different source (ES vs Domain)
     * - Skip domain validation (already stored)
     * - Direct conversion for performance
     */
    public static EventDto fromDocument(EventDocument doc) {
        return new EventDto(
                doc.getId(),
                doc.getTenantId(),
                doc.getAppId(),
                doc.getProfileId(),
                doc.getEventName(),
                doc.getEventTime(),
                doc.getNormalizedTime(),
                doc.getProperties(),
                doc.getTraits(),
                doc.getContext()
        );
    }
}