package com.vft.cdp.event.infra.es;

import com.vft.cdp.event.domain.model.Event;
import com.vft.cdp.event.domain.repository.EventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.Optional;

/**
 * Elasticsearch implementation of EventRepository
 *
 * ARCHITECTURE PATTERN:
 * Domain Interface → Infrastructure Implementation
 *
 * RESPONSIBILITIES:
 * 1. Implement domain repository contract
 * 2. Delegate to Spring Data repository
 * 3. Convert Domain ↔ ES Document using Mapper
 *
 * WHY THIS LAYER?
 * - Domain doesn't depend on ES
 * - Easy to swap implementation (ES → Cassandra)
 * - Centralized mapping logic
 */
@Slf4j
@Repository  // Spring: Mark as repository bean
@RequiredArgsConstructor  // Lombok: Constructor for final fields
public class EsEventRepository implements EventRepository {

    /**
     * Spring Data repository (auto-implemented by Spring)
     */
    private final SpringDataEventEsRepository springDataRepo;

    /**
     * Save event
     *
     * FLOW:
     * 1. Domain Event → ES Document (via Mapper)
     * 2. Save to ES (via Spring Data)
     * 3. ES Document → Domain Event (via Mapper)
     *
     * WHY CONVERT BACK?
     * - Consistent return type (Domain)
     * - May have generated fields from ES
     */
    @Override
    public Event save(Event event) {
        log.debug("Saving event: id={}, tenant={}, profile={}",
                event.getId(), event.getTenantId(), event.getProfileId());

        // Domain → Document
        EventDocument doc = EventMapper.toDocument(event);

        // Save to ES
        EventDocument saved = springDataRepo.save(doc);

        // Document → Domain
        return EventMapper.toDomain(saved);
    }

    /**
     * Find by ID
     *
     * FLOW:
     * 1. Query ES by ID
     * 2. If found: ES Document → Domain Event
     * 3. If not found: return Optional.empty()
     */
    @Override
    public Optional<Event> findById(String eventId) {
        return springDataRepo.findById(eventId)
                .map(EventMapper::toDomain);  // Method reference
    }

    /**
     * Find by profile (paginated)
     *
     * FLOW:
     * 1. Query ES with pagination
     * 2. Get Page<EventDocument>
     * 3. Convert each document → domain
     * 4. Return Page<Event>
     */
    @Override
    public Page<Event> findByProfile(
            String tenantId,
            String profileId,
            Pageable pageable) {

        log.debug("Finding events: tenant={}, profile={}, page={}, size={}",
                tenantId, profileId, pageable.getPageNumber(), pageable.getPageSize());

        return springDataRepo
                .findByTenantIdAndProfileId(tenantId, profileId, pageable)
                .map(EventMapper::toDomain);  // Transform each element
    }

    /**
     * Find by profile and time range
     */
    @Override
    public Page<Event> findByProfileAndTimeRange(
            String tenantId,
            String profileId,
            Instant startTime,
            Instant endTime,
            Pageable pageable) {

        log.debug("Finding events in range: tenant={}, profile={}, start={}, end={}",
                tenantId, profileId, startTime, endTime);

        return springDataRepo
                .findByTenantIdAndProfileIdAndNormalizedTimeBetween(
                        tenantId, profileId, startTime, endTime, pageable)
                .map(EventMapper::toDomain);
    }

    /**
     * Count events
     */
    @Override
    public long countByProfile(String tenantId, String profileId) {
        return springDataRepo.countByTenantIdAndProfileId(tenantId, profileId);
    }
}