package com.vft.cdp.event.domain.repository;

import com.vft.cdp.event.domain.model.Event;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.Instant;
import java.util.Optional;

/**
 * Event Repository Interface - Domain contract
 *
 * WHY INTERFACE IN DOMAIN?
 * - Dependency Inversion: Domain defines contract, Infrastructure implements
 * - Testability: Easy to mock in unit tests
 * - Flexibility: Swap implementation (ES → Cassandra) without changing domain
 */
public interface EventRepository {

    /**
     * Save event to storage
     *
     * @return saved event (with generated fields if any)
     */
    Event save(Event event);

    /**
     * Find event by unique ID
     */
    Optional<Event> findById(String eventId);

    /**
     * Get all events for a profile (paginated)
     */
    Page<Event> findByProfile(
            String tenantId,
            String profileId,
            Pageable pageable
    );

    /**
     * Get events in time range for a profile
     */
    Page<Event> findByProfileAndTimeRange(
            String tenantId,
            String profileId,
            Instant startTime,
            Instant endTime,
            Pageable pageable
    );

    /**
     * Count total events for a profile
     */
    long countByProfile(String tenantId, String profileId);
}
/*

**💡 TẠI SAO CHỈ LÀ INTERFACE?**
┌──────────────────────────────────────────────────┐
│ DEPENDENCY INVERSION PRINCIPLE                   │
└──────────────────────────────────────────────────┘

WRONG (High coupling):
Domain → depends on → Infrastructure
  ↓
Event.java → uses ES classes directly
  → Khó test, khó thay đổi DB

RIGHT (Loose coupling):
Domain → defines → EventRepository interface
  ↑                       ↑
          |                       implements
          |                       ↓
Application          EsEventRepository (Infrastructure)

→ Domain không biết gì về ES
→ Test domain không cần ES
→ Đổi DB chỉ sửa Infrastructure layer

 */