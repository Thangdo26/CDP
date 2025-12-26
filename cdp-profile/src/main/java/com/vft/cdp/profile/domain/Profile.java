package com.vft.cdp.profile.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE DOMAIN ENTITY
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Core business entity representing a user profile.
 * Contains all business rules and invariants.
 *
 * DDD Pattern: This is the DOMAIN MODEL, NOT DTO or Document
 * - Used ONLY in domain and application layers
 * - Infrastructure layer maps to/from this entity
 * - API layer never touches this directly
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Profile {

    /**
     * Composite ID: {tenant_id}|{app_id}|{user_id}
     */
    private ProfileId profileId;

    /**
     * Profile type (e.g., registration, ekyc, purchase)
     */
    private String type;

    /**
     * Profile status
     * - ACTIVE: Can be merged
     * - MERGED: Already merged into master
     * - DELETED: Soft deleted
     */
    @Builder.Default
    private ProfileStatus status = ProfileStatus.ACTIVE;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * User traits (personal information)
     */
    private Traits traits;

    /**
     * Platform/device information
     */
    private Platforms platforms;

    /**
     * Campaign/UTM tracking
     */
    private Campaign campaign;

    /**
     * Custom metadata
     */
    private Map<String, Object> metadata;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MERGE TRACKING
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Master profile ID if merged
     * Only set when status = MERGED
     */
    private String mergedToMasterId;

    /**
     * When profile was merged
     */
    private Instant mergedAt;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SYSTEM METADATA
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String partitionKey;
    private Instant enrichedAt;
    private String enrichedId;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TIMESTAMPS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Instant createdAt;
    private Instant updatedAt;
    private Instant firstSeenAt;
    private Instant lastSeenAt;
    private Integer version;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECTS (Inner Classes)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Profile ID Value Object
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ProfileId {
        private String tenantId;
        private String appId;
        private String userId;

        public String toCompositeId() {
            return tenantId + "|" + appId + "|" + userId;
        }

        public static ProfileId fromCompositeId(String compositeId) {
            String[] parts = compositeId.split("\\|");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid composite ID: " + compositeId);
            }
            return new ProfileId(parts[0], parts[1], parts[2]);
        }
    }

    /**
     * Traits Value Object
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Traits {
        private String fullName;
        private String firstName;
        private String lastName;
        private String idcard;
        private String oldIdcard;
        private String phone;
        private String email;
        private String gender;
        private String dob;
        private String address;
        private String religion;
    }

    /**
     * Platforms Value Object
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Platforms {
        private String os;
        private String device;
        private String browser;
        private String appVersion;
    }

    /**
     * Campaign Value Object
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Campaign {
        private String utmSource;
        private String utmCampaign;
        private String utmMedium;
        private String utmContent;
        private String utmTerm;
        private String utmCustom;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOMAIN METHODS (Business Logic)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Mark profile as merged
     *
     * Business rule: Only ACTIVE profiles can be merged
     */
    public void markAsMerged(String masterProfileId) {
        if (this.status != ProfileStatus.ACTIVE) {
            throw new IllegalStateException(
                    "Only ACTIVE profiles can be merged. Current status: " + this.status
            );
        }

        this.status = ProfileStatus.MERGED;
        this.mergedToMasterId = masterProfileId;
        this.mergedAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    /**
     * Check if profile can be merged
     */
    public boolean canBeMerged() {
        return this.status == ProfileStatus.ACTIVE;
    }

    /**
     * Check if profile is already merged
     */
    public boolean isMerged() {
        return this.status == ProfileStatus.MERGED;
    }

    /**
     * Update profile data (business logic)
     */
    public void update(Traits newTraits, Platforms newPlatforms, Campaign newCampaign) {
        if (this.status == ProfileStatus.DELETED) {
            throw new IllegalStateException("Cannot update DELETED profile");
        }

        if (newTraits != null) {
            this.traits = mergeTraits(this.traits, newTraits);
        }

        if (newPlatforms != null) {
            this.platforms = newPlatforms;
        }

        if (newCampaign != null) {
            this.campaign = newCampaign;
        }

        this.updatedAt = Instant.now();
        this.lastSeenAt = Instant.now();
        this.version = (this.version != null ? this.version : 0) + 1;
    }

    /**
     * Merge traits (business logic)
     */
    private Traits mergeTraits(Traits existing, Traits incoming) {
        if (existing == null) return incoming;
        if (incoming == null) return existing;

        return Traits.builder()
                .fullName(incoming.getFullName() != null ? incoming.getFullName() : existing.getFullName())
                .firstName(incoming.getFirstName() != null ? incoming.getFirstName() : existing.getFirstName())
                .lastName(incoming.getLastName() != null ? incoming.getLastName() : existing.getLastName())
                .idcard(incoming.getIdcard() != null ? incoming.getIdcard() : existing.getIdcard())
                .oldIdcard(incoming.getOldIdcard() != null ? incoming.getOldIdcard() : existing.getOldIdcard())
                .phone(incoming.getPhone() != null ? incoming.getPhone() : existing.getPhone())
                .email(incoming.getEmail() != null ? incoming.getEmail() : existing.getEmail())
                .gender(incoming.getGender() != null ? incoming.getGender() : existing.getGender())
                .dob(incoming.getDob() != null ? incoming.getDob() : existing.getDob())
                .address(incoming.getAddress() != null ? incoming.getAddress() : existing.getAddress())
                .religion(incoming.getReligion() != null ? incoming.getReligion() : existing.getReligion())
                .build();
    }

    /**
     * Soft delete profile
     */
    public void delete() {
        if (this.status == ProfileStatus.MERGED) {
            throw new IllegalStateException("Cannot delete MERGED profile");
        }

        this.status = ProfileStatus.DELETED;
        this.updatedAt = Instant.now();
    }

    public String getTenantId() {
        return profileId != null ? profileId.getTenantId() : null;
    }

    public String getAppId() {
        return profileId != null ? profileId.getAppId() : null;
    }

    public String getUserId() {
        return profileId != null ? profileId.getUserId() : null;
    }
}