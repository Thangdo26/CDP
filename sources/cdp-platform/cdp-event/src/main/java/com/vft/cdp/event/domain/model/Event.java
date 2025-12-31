package com.vft.cdp.event.domain.model;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Event domain entity - Pure Java, no framework dependencies
 *
 * WHY PURE JAVA?
 * - Easy to test (no need for Spring context)
 * - Framework-independent (can migrate to different tech stack)
 * - Clear business logic without infrastructure noise
 */
public class Event {

    // === IMMUTABLE FIELDS (final) ===
    private final String id;              // enrichedId
    private final String tenantId;
    private final String appId;
    private final String profileId;
    private final String eventName;
    private final Instant eventTime;      // Client time
    private final Instant normalizedTime;        // Server time

    // === DYNAMIC FIELDS (can be null) ===
    private Map<String, Object> properties;
    private Map<String, Object> traits;
    private Map<String, Object> context;

    /**
     * CONSTRUCTOR với validation
     *
     * WHY VALIDATE IN CONSTRUCTOR?
     * - Fail fast: Không cho tạo object invalid
     * - Domain integrity: Đảm bảo business rules
     */
    public Event(String id,
                 String tenantId,
                 String appId,
                 String profileId,
                 String eventName,
                 Instant eventTime,
                 Instant normalizedTime,
                 Map<String, Object> properties,
                 Map<String, Object> traits,
                 Map<String, Object> context) {

        // Validation
        this.id = Objects.requireNonNull(id, "Event ID is required");
        this.tenantId = Objects.requireNonNull(tenantId, "Tenant ID is required");
        this.appId = Objects.requireNonNull(appId, "App ID is required");
        this.profileId = Objects.requireNonNull(profileId, "Profile ID is required");
        this.eventName = Objects.requireNonNull(eventName, "Event name is required");
        this.eventTime = Objects.requireNonNull(eventTime, "Event time is required");
        this.normalizedTime = Objects.requireNonNull(normalizedTime, "Normalized time is required");

        // Defensive copy - Tránh external modification
        this.properties = properties != null ? new HashMap<>(properties) : new HashMap<>();
        this.traits = traits != null ? new HashMap<>(traits) : new HashMap<>();
        this.context = context != null ? new HashMap<>(context) : new HashMap<>();
    }

    /**
     * FACTORY METHOD để tạo Event từ common event
     *
     * WHY FACTORY METHOD?
     * - Encapsulation: Logic tạo object nằm trong domain
     * - Flexibility: Dễ thay đổi cách construct
     * - Readability: Event.fromEnrichedEvent() rõ ràng hơn new Event(...)
     */
    public static Event fromEnrichedEvent(
            com.vft.cdp.common.event.EnrichedEvent enriched) {

        return new Event(
                enriched.getEnrichedId(),
                enriched.getTenantId(),
                enriched.getAppId(),
                enriched.getProfileId(),
                enriched.getEventName(),
                enriched.getEventTime(),
                enriched.getNormalizedTime(),
                enriched.getProperties(),
                enriched.getTraits(),
                enriched.getContext()
        );
    }

    // ============================================
    // GETTERS ONLY (No setters - Immutable)
    // ============================================

    public String getId() {
        return id;
    }

    public String getTenantId() {
        return tenantId;
    }

    public String getAppId() {
        return appId;
    }

    public String getProfileId() {
        return profileId;
    }

    public String getEventName() {
        return eventName;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public Instant getNormalizedTime() {
        return normalizedTime;
    }

    /**
     * DEFENSIVE COPY trong getter
     *
     * WHY?
     * - Prevent external modification
     * - Maintain immutability
     */
    public Map<String, Object> getProperties() {
        return new HashMap<>(properties);
    }

    public Map<String, Object> getTraits() {
        return new HashMap<>(traits);
    }

    public Map<String, Object> getContext() {
        return new HashMap<>(context);
    }

    // ============================================
    // EQUALS & HASHCODE (dựa trên ID)
    // ============================================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return Objects.equals(id, event.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", tenantId='" + tenantId + '\'' +
                ", profileId='" + profileId + '\'' +
                ", eventName='" + eventName + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}