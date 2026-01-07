package com.vft.cdp.event.api;

import com.vft.cdp.event.application.EventService;
import com.vft.cdp.event.application.dto.EventDto;
import com.vft.cdp.event.application.query.GetProfileEventsQuery;
import com.vft.cdp.event.api.response.EventListResponse;
import com.vft.cdp.event.api.response.EventResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;

/**
 * REST API Controller for Events
 *
 * RESPONSIBILITIES:
 * - Handle HTTP requests/responses
 * - Validate input (with @Valid)
 * - Call application service
 * - Convert DTO → Response
 * - Set HTTP status codes
 *
 * NOT RESPONSIBLE FOR:
 * - Business logic (→ Service)
 * - Data access (→ Repository)
 * - Mapping domain objects (→ Mapper)
 */
@Slf4j
@RestController  // = @Controller + @ResponseBody on all methods
@RequestMapping("/v1/events")  // Base path
@RequiredArgsConstructor  // Constructor injection
public class EventController {

    private final EventService eventService;

    /**
     * Get events for a profile
     *
     * ENDPOINT: GET /v1/events/{tenantId}/{profileId}
     *
     * ANNOTATIONS EXPLAINED:
     *
     * @GetMapping: HTTP GET request
     * @PathVariable: Extract from URL path
     * @RequestParam: Query parameters (?page=0&size=20)
     *
     * EXAMPLE REQUEST:
     * GET /v1/events/tenant-demo/uid:user_123?page=0&size=20
     * Headers:
     *   X-API-Key: cdp_test123_secret
     */
    @GetMapping("/{tenantId}/{profileId}")
    public ResponseEntity<EventListResponse> getProfileEvents(
            @PathVariable("tenantId") String tenantId,
            @PathVariable("profileId") String profileId,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size
    ) {
        log.info("GET /v1/events/{}/{} - page={}, size={}",
                tenantId, profileId, page, size);

        // Create query object
        GetProfileEventsQuery query = GetProfileEventsQuery.create(
                tenantId, profileId, page, size);

        // Call service
        Page<EventDto> eventPage = eventService.getProfileEvents(query);

        // Convert DTO → Response
        Page<EventResponse> responsePage = eventPage.map(EventResponse::fromDto);

        // Wrap in list response
        EventListResponse response = EventListResponse.fromPage(responsePage);

        // Return with 200 OK
        return ResponseEntity.ok(response);
    }

    /**
     * Get events in time range
     *
     * ENDPOINT: GET /v1/events/{tenantId}/{profileId}/range
     *
     * QUERY PARAMETERS:
     * - startTime: ISO 8601 datetime (2025-12-01T00:00:00Z)
     * - endTime: ISO 8601 datetime (2025-12-17T23:59:59Z)
     * - page: Page number (default 0)
     * - size: Page size (default 20)
     *
     * @DateTimeFormat: Auto-convert String → Instant
     *
     * EXAMPLE REQUEST:
     * GET /v1/events/tenant-demo/uid:user_123/range?
     *     startTime=2025-12-01T00:00:00Z&
     *     endTime=2025-12-17T23:59:59Z&
     *     page=0&size=50
     */
    @GetMapping("/{tenantId}/{profileId}/range")
    public ResponseEntity<EventListResponse> getProfileEventsInRange(
            @PathVariable String tenantId,
            @PathVariable String profileId,
            @RequestParam
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant startTime,
            @RequestParam
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            Instant endTime,
            @RequestParam(value = "page", defaultValue = "0") int page,
            @RequestParam(value = "size", defaultValue = "20") int size
    ) {
        log.info("GET /v1/events/{}/{}/range - start={}, end={}, page={}, size={}",
                tenantId, profileId, startTime, endTime, page, size);

        // Create query with time range
        GetProfileEventsQuery query = new GetProfileEventsQuery(
                tenantId, profileId, page, size, startTime, endTime);

        // Call service
        Page<EventDto> eventPage = eventService.getProfileEvents(query);

        // Convert & return
        Page<EventResponse> responsePage = eventPage.map(EventResponse::fromDto);
        EventListResponse response = EventListResponse.fromPage(responsePage);

        return ResponseEntity.ok(response);
    }

    /**
     * Get single event by ID
     *
     * ENDPOINT: GET /v1/events/{eventId}
     */
    @GetMapping("/{eventId}")
    public ResponseEntity<EventResponse> getEventById(
            @PathVariable String eventId
    ) {
        log.info("GET /v1/events/{}", eventId);

        try {
            EventDto dto = eventService.getEventById(eventId);
            EventResponse response = EventResponse.fromDto(dto);
            return ResponseEntity.ok(response);

        } catch (EventService.EventNotFoundException ex) {
            log.warn("Event not found: {}", eventId);
            return ResponseEntity.notFound().build();  // 404
        }
    }

    /**
     * Count events for profile
     *
     * ENDPOINT: GET /v1/events/{tenantId}/{profileId}/count
     */
    @GetMapping("/{tenantId}/{profileId}/count")
    public ResponseEntity<Long> countProfileEvents(
            @PathVariable String tenantId,
            @PathVariable String profileId
    ) {
        log.info("GET /v1/events/{}/{}/count", tenantId, profileId);

        long count = eventService.countProfileEvents(tenantId, profileId);
        return ResponseEntity.ok(count);
    }
}