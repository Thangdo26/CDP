package com.vft.cdp.profile.domain;

import com.vft.cdp.profile.application.model.ProfileModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE DOMAIN ENTITY - PURE DDD
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ Self-contained domain entity
 * ✅ No dependency on cdp-common
 * ✅ Implements ProfileModel interface
 * ✅ Contains business logic
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Profile implements ProfileModel {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // IDENTITY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String tenantId;
    private String appId;
    private String userId;
    private String type;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Builder.Default
    private ProfileStatus status = ProfileStatus.ACTIVE;

    private String mergedToMasterId;
    private Instant mergedAt;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Traits traits;
    private Platforms platforms;
    private Campaign campaign;
    private Map<String, Object> metadata;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TIMESTAMPS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Instant createdAt;
    private Instant updatedAt;
    private Instant firstSeenAt;
    private Instant lastSeenAt;
    private Integer version;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ProfileModel INTERFACE IMPLEMENTATION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public String getStatus() {
        return status != null ? status.getValue() : ProfileStatus.ACTIVE.getValue();
    }

    @Override
    public ProfileModel.TraitsModel getTraits() {
        return traits;
    }

    @Override
    public ProfileModel.PlatformsModel getPlatforms() {
        return platforms;
    }

    @Override
    public ProfileModel.CampaignModel getCampaign() {
        return campaign;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECTS (Inner Classes)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Traits implements ProfileModel.TraitsModel {
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Platforms implements ProfileModel.PlatformsModel {
        private String os;
        private String device;
        private String browser;
        private String appVersion;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Campaign implements ProfileModel.CampaignModel {
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
     * Check if profile is merged
     */
    public boolean isMerged() {
        return this.status == ProfileStatus.MERGED;
    }

    /**
     * Update profile data
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
     * Merge traits
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
     * Soft delete
     */
    public void delete() {
        if (this.status == ProfileStatus.MERGED) {
            throw new IllegalStateException("Cannot delete MERGED profile");
        }

        this.status = ProfileStatus.DELETED;
        this.updatedAt = Instant.now();
    }

    /**
     * Restore deleted profile
     */
    public void restore() {
        if (this.status != ProfileStatus.DELETED) {
            throw new IllegalStateException("Only DELETED profiles can be restored");
        }

        this.status = ProfileStatus.ACTIVE;
        this.updatedAt = Instant.now();
    }

    /**
     * Factory: Create from command
     */
    public static Profile create(
            String tenantId,
            String appId,
            String userId,
            String type,
            Traits traits,
            Platforms platforms,
            Campaign campaign,
            Map<String, Object> metadata) {

        Instant now = Instant.now();

        return Profile.builder()
                .tenantId(tenantId)
                .appId(appId)
                .userId(userId)
                .type(type)
                .status(ProfileStatus.ACTIVE)
                .traits(traits)
                .platforms(platforms)
                .campaign(campaign)
                .metadata(metadata)
                .createdAt(now)
                .updatedAt(now)
                .firstSeenAt(now)
                .lastSeenAt(now)
                .version(1)
                .build();
    }
}