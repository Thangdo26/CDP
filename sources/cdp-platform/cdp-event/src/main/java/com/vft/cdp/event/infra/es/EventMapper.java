package com.vft.cdp.event.infra.es;

import com.vft.cdp.common.event.EnrichedEvent;
import com.vft.cdp.event.domain.model.Event;

/**
 * Mapper between Domain ↔ Infrastructure
 *
 * WHY MAPPER?
 * - Separation: Domain doesn't know about ES
 * - Flexibility: Easy to change DB implementation
 * - Testability: Can test domain without ES
 *
 * PATTERN: Bidirectional mapping
 * - toDocument: Domain → ES Document
 * - toDomain: ES Document → Domain
 * - fromEnrichedEvent: Common Event → ES Document
 */
public final class EventMapper {

    /**
     * Private constructor - Utility class pattern
     *
     * WHY?
     * - Prevent instantiation: EventMapper mapper = new EventMapper() ❌
     * - All methods are static
     * - No state to maintain
     */
    private EventMapper() {
        throw new AssertionError("Utility class cannot be instantiated");
    }

    /**
     * Map Domain Entity → ES Document
     *
     * WHEN TO USE?
     * - Saving domain object to ES
     * - After business logic processing
     *
     * FLOW:
     * Service → Domain Event → toDocument() → ES Document → Save to ES
     */
    public static EventDocument toDocument(Event event) {
        EventDocument doc = new EventDocument();
        doc.setId(event.getId());
        doc.setTenantId(event.getTenantId());
        doc.setAppId(event.getAppId());
        doc.setProfileId(event.getProfileId());
        doc.setEventName(event.getEventName());
        doc.setEventTime(event.getEventTime());
        doc.setNormalizedTime(event.getNormalizedTime());
        doc.setProperties(event.getProperties());
        doc.setTraits(event.getTraits());
        doc.setContext(event.getContext());
        return doc;
    }

    /**
     * Map ES Document → Domain Entity
     *
     * WHEN TO USE?
     * - Reading from ES
     * - Query results need to be converted to domain
     *
     * FLOW:
     * ES Query → ES Document → toDomain() → Domain Event → Service
     */
    public static Event toDomain(EventDocument doc) {
        if (doc == null) {
            return null;
        }

        return new Event(
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

    /**
     * Map Common EnrichedEvent → ES Document
     *
     * WHY DIRECT MAPPING?
     * - Performance: Skip domain validation when storing from Kafka
     * - Convenience: Common event already validated by inbound processor
     *
     * WHEN TO USE?
     * - Kafka consumer storing events directly
     *
     * FLOW:
     * Kafka → EnrichedEvent → fromEnrichedEvent() → ES Document → Save
     */
    public static EventDocument fromEnrichedEvent(EnrichedEvent enriched) {
        EventDocument doc = new EventDocument();
        doc.setId(enriched.getEnrichedId());
        doc.setTenantId(enriched.getTenantId());
        doc.setAppId(enriched.getAppId());
        doc.setProfileId(enriched.getProfileId());
        doc.setEventName(enriched.getEventName());
        doc.setEventTime(enriched.getEventTime());
        doc.setNormalizedTime(enriched.getNormalizedTime());
        doc.setProperties(enriched.getProperties());
        doc.setTraits(enriched.getTraits());
        doc.setContext(enriched.getContext());
        return doc;
    }
}