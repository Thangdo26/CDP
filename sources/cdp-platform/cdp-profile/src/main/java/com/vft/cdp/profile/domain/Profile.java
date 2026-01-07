package com.vft.cdp.profile.domain;

import com.vft.cdp.profile.application.model.ProfileModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.*;

import lombok.extern.slf4j.Slf4j;

/*
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE DOMAIN ENTITY - PURE DDD
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 *  Self-contained domain entity
 *  No dependency on cdp-common
 *  Implements ProfileModel interface
 *  Contains business logic
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Profile implements ProfileModel {

    private String tenantId;
    private String appId;
    private String userId;
    private String type;
    private ProfileStatus status;
    private String mergedToMasterId;
    private Instant mergedAt;

    private List<UserIdentity> users;

    private Traits traits;
    private Platforms platforms;
    private Campaign campaign;
    private Map<String, Object> metadata;

    private Instant createdAt;
    private Instant updatedAt;
    private Instant firstSeenAt;
    private Instant lastSeenAt;
    private Integer version;

    @Override
    public String getStatus() {
        return status != null ? status.name() : null;
    }

    // ═══════════════════════════════════════════════════════════════
    // UPDATE METHODS - FIXED
    // ═══════════════════════════════════════════════════════════════

    /**
     * Update profile data WITHOUT touching updatedAt
     * Caller is responsible for setting updatedAt explicitly
     */
    public void update(Traits newTraits, Platforms newPlatforms, Campaign newCampaign) {
        if (newTraits != null) {
            this.traits = mergeTraits(this.traits, newTraits);
        }
        if (newPlatforms != null) {
            this.platforms = mergePlatforms(this.platforms, newPlatforms);
        }
        if (newCampaign != null) {
            this.campaign = mergeCampaign(this.campaign, newCampaign);
        }

        // FIXED: Do NOT set updatedAt here
        // Let ProfileTrackService control the timestamp based on metadata

        this.version = (this.version == null ? 0 : this.version) + 1;

        // Update lastSeenAt to current time (this is different from updatedAt)
        this.lastSeenAt = Instant.now();
    }

    /**
     * Explicitly set updatedAt (used by ProfileTrackService)
     */
    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * Update metadata - merge with existing
     */
    public void updateMetadata(Map<String, Object> newMetadata) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        if (newMetadata != null) {
            this.metadata.putAll(newMetadata);
        }
    }

    /**
     * Soft delete
     */
    public void delete() {
        this.status = ProfileStatus.DELETED;
        this.updatedAt = Instant.now();
    }

    // ═══════════════════════════════════════════════════════════════
    // MERGE HELPERS - Keep non-null values
    // ═══════════════════════════════════════════════════════════════

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

    private Platforms mergePlatforms(Platforms existing, Platforms incoming) {
        if (existing == null) return incoming;
        if (incoming == null) return existing;

        return Platforms.builder()
                .os(incoming.getOs() != null ? incoming.getOs() : existing.getOs())
                .device(incoming.getDevice() != null ? incoming.getDevice() : existing.getDevice())
                .browser(incoming.getBrowser() != null ? incoming.getBrowser() : existing.getBrowser())
                .appVersion(incoming.getAppVersion() != null ? incoming.getAppVersion() : existing.getAppVersion())
                .build();
    }

    private Campaign mergeCampaign(Campaign existing, Campaign incoming) {
        if (existing == null) return incoming;
        if (incoming == null) return existing;

        return Campaign.builder()
                .utmSource(incoming.getUtmSource() != null ? incoming.getUtmSource() : existing.getUtmSource())
                .utmCampaign(incoming.getUtmCampaign() != null ? incoming.getUtmCampaign() : existing.getUtmCampaign())
                .utmMedium(incoming.getUtmMedium() != null ? incoming.getUtmMedium() : existing.getUtmMedium())
                .utmContent(incoming.getUtmContent() != null ? incoming.getUtmContent() : existing.getUtmContent())
                .utmTerm(incoming.getUtmTerm() != null ? incoming.getUtmTerm() : existing.getUtmTerm())
                .utmCustom(incoming.getUtmCustom() != null ? incoming.getUtmCustom() : existing.getUtmCustom())
                .build();
    }

    /**
     * Add a user identity to this profile
     */
    public void addUser(String appId, String userId) {
        if (this.users == null) {
            this.users = new ArrayList<>();
        }

        // Check if already exists
        boolean exists = this.users.stream()
                .anyMatch(u -> u.getAppId().equals(appId) && u.getUserId().equals(userId));

        if (!exists) {
            this.users.add(UserIdentity.builder()
                    .appId(appId)
                    .userId(userId)
                    .build());
            log.debug("Added user to profile: {}|{}", appId, userId);
        }
    }

    /**
     * Remove a user identity from this profile
     */
    public void removeUser(String appId, String userId) {
        if (this.users != null) {
            boolean removed = this.users.removeIf(u ->
                    u.getAppId().equals(appId) && u.getUserId().equals(userId));

            if (removed) {
                log.debug("🗑️ Removed user from profile: {}|{}", appId, userId);
            }
        }
    }

    /**
     * Check if user is linked to this profile
     */
    public boolean hasUser(String appId, String userId) {
        if (this.users == null) {
            return false;
        }
        return this.users.stream()
                .anyMatch(u -> u.getAppId().equals(appId) && u.getUserId().equals(userId));
    }

    // ═══════════════════════════════════════════════════════════════
    // INNER CLASSES
    // ═══════════════════════════════════════════════════════════════

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserIdentity implements ProfileModel.UserIdentityModel {
        private String appId;
        private String userId;
    }


    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
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
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Platforms implements ProfileModel.PlatformsModel {
        private String os;
        private String device;
        private String browser;
        private String appVersion;
    }

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Campaign implements ProfileModel.CampaignModel {
        private String utmSource;
        private String utmCampaign;
        private String utmMedium;
        private String utmContent;
        private String utmTerm;
        private String utmCustom;
    }

    // ═══════════════════════════════════════════════════════════════
    // FACTORY METHODS
    // ═══════════════════════════════════════════════════════════════

    public static Profile create(
            String tenantId,
            String appId,
            String userId,
            String type,
            Traits traits,
            Platforms platforms,
            Campaign campaign,
            Map<String, Object> metadata
    ) {
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
                .metadata(metadata != null ? metadata : new HashMap<>())
                .createdAt(now)
                .updatedAt(now)
                .firstSeenAt(now)
                .lastSeenAt(now)
                .version(1)
                .build();
    }
}