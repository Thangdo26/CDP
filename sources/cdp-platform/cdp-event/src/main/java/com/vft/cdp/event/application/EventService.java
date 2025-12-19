package com.vft.cdp.event.application;

import com.vft.cdp.event.application.dto.EventDto;
import com.vft.cdp.event.application.query.GetProfileEventsQuery;
import com.vft.cdp.event.domain.model.Event;
import com.vft.cdp.event.domain.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

/**
 * Event Application Service - Use case orchestration
 *
 * RESPONSIBILITIES:
 * - Orchestrate use cases
 * - Coordinate domain objects
 * - Convert Domain → DTO
 * - Transaction boundaries (if needed)
 *
 * NOT RESPONSIBLE FOR:
 * - Business logic (→ Domain)
 * - Data access (→ Repository)
 * - HTTP handling (→ Controller)
 */
@Slf4j
@Service
@RequiredArgsConstructor  // Lombok: Generate constructor for final fields
public class EventService {

    private final EventRepository eventRepository;

    /**
     * Get events for a profile
     *
     * FLOW:
     * 1. Create PageRequest from query
     * 2. Call repository (Domain)
     * 3. Convert Domain → DTO
     * 4. Return Page<DTO>
     */
    public Page<EventDto> getProfileEvents(GetProfileEventsQuery query) {
        log.debug("Getting events: tenant={}, profile={}, page={}",
                query.tenantId(), query.profileId(), query.page());

        // Create pagination + sorting
        PageRequest pageRequest = PageRequest.of(
                query.page(),
                query.size(),
                Sort.by("normalizedTime").descending()  // Latest first
        );

        // Query repository
        Page<Event> events;
        if (query.hasTimeRange()) {
            events = eventRepository.findByProfileAndTimeRange(
                    query.tenantId(),
                    query.profileId(),
                    query.startTime(),
                    query.endTime(),
                    pageRequest
            );
        } else {
            events = eventRepository.findByProfile(
                    query.tenantId(),
                    query.profileId(),
                    pageRequest
            );
        }

        // Convert Domain → DTO
        return events.map(EventDto::fromDomain);
    }

    /**
     * Get single event by ID
     */
    public EventDto getEventById(String eventId) {
        Event event = eventRepository.findById(eventId)
                .orElseThrow(() -> new EventNotFoundException(eventId));

        return EventDto.fromDomain(event);
    }

    /**
     * Count total events for profile
     */
    public long countProfileEvents(String tenantId, String profileId) {
        return eventRepository.countByProfile(tenantId, profileId);
    }

    /**
     * Custom exception
     */
    public static class EventNotFoundException extends RuntimeException {
        public EventNotFoundException(String eventId) {
            super("Event not found: " + eventId);
        }
    }
}