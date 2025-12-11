package com.vft.cdp.profile.domain.model;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Profile {

    private final String tenantId;
    private final String profileId;

    private Map<String, Object> identifiers;   // email, phone, deviceId...
    private Map<String, Object> traits;        // thuộc tính customer
    private String status;                     // ACTIVE / DELETED / ...

    private Instant createdAt;
    private Instant updatedAt;

    public Profile(String tenantId,
                   String profileId,
                   Map<String, Object> identifiers,
                   Map<String, Object> traits,
                   String status,
                   Instant createdAt,
                   Instant updatedAt) {

        this.tenantId = Objects.requireNonNull(tenantId);
        this.profileId = Objects.requireNonNull(profileId);
        this.identifiers = identifiers != null ? new HashMap<>(identifiers) : new HashMap<>();
        this.traits = traits != null ? new HashMap<>(traits) : new HashMap<>();
        this.status = status != null ? status : "ACTIVE";
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static Profile newProfile(String tenantId,
                                     String profileId,
                                     Map<String, Object> identifiers,
                                     Map<String, Object> traits) {
        Instant now = Instant.now();
        return new Profile(tenantId, profileId, identifiers, traits, "ACTIVE", now, now);
    }

    public void mergeTraits(Map<String, Object> newTraits) {
        if (newTraits == null || newTraits.isEmpty()) {
            return;
        }
        this.traits.putAll(newTraits);
        touch();
    }

    public void mergeIdentifiers(Map<String, Object> newIdentifiers) {
        if (newIdentifiers == null || newIdentifiers.isEmpty()) {
            return;
        }
        this.identifiers.putAll(newIdentifiers);
        touch();
    }

    public void markDeleted() {
        this.status = "DELETED";
        touch();
    }

    private void touch() {
        this.updatedAt = Instant.now();
    }

    // Getters only (no setters để giữ bất biến)
    public String getTenantId() {
        return tenantId;
    }

    public String getProfileId() {
        return profileId;
    }

    public Map<String, Object> getIdentifiers() {
        return new HashMap<>(identifiers);
    }

    public Map<String, Object> getTraits() {
        return new HashMap<>(traits);
    }

    public String getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
