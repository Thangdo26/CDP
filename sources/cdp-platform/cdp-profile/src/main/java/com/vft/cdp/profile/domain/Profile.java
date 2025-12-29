package com.vft.cdp.profile.domain;

import com.vft.cdp.profile.application.model.ProfileModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import java.util.Objects;

/**
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
     *
     *  NEW: Auto-reactivate if data changed
     *  NEW: Returns masterId if profile was reactivated from MERGED status
     *
     * @return masterId if profile was reactivated, null otherwise
     */
    public String update(Traits newTraits, Platforms newPlatforms, Campaign newCampaign) {
        if (this.status == ProfileStatus.DELETED) {
            throw new IllegalStateException("Cannot update DELETED profile");
        }

        boolean hasChanges = false;
        String reactivatedFromMasterId = null;

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // CHECK AND MERGE TRAITS
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (newTraits != null) {
            Traits mergedTraits = mergeTraits(this.traits, newTraits);

            if (!traitsEquals(this.traits, mergedTraits)) {
                this.traits = mergedTraits;
                hasChanges = true;
                log.debug("  ✏️  Traits changed");
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // CHECK AND UPDATE PLATFORMS
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (newPlatforms != null && !platformsEquals(this.platforms, newPlatforms)) {
            this.platforms = newPlatforms;
            hasChanges = true;
            log.debug("  ✏️  Platforms changed");
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // CHECK AND UPDATE CAMPAIGN
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (newCampaign != null && !campaignEquals(this.campaign, newCampaign)) {
            this.campaign = newCampaign;
            hasChanges = true;
            log.debug("  ✏️  Campaign changed");
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // AUTO-REACTIVATE IF DATA CHANGED
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (hasChanges && this.status == ProfileStatus.MERGED) {
            log.info("🔄 Profile data changed - auto-reactivating from MERGED to ACTIVE: {}|{}|{}",
                    this.tenantId, this.appId, this.userId);

            reactivatedFromMasterId = this.mergedToMasterId;
            this.status = ProfileStatus.ACTIVE;
            this.mergedToMasterId = null;
            this.mergedAt = null;
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // UPDATE TIMESTAMPS - ONLY IF DATA CHANGED
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (hasChanges) {
            this.updatedAt = Instant.now();
            this.version = (this.version != null ? this.version : 0) + 1;

        }

        return reactivatedFromMasterId;
    }

    /**
     *  NEW: Update metadata and extract timestamps
     *
     * Call this AFTER update() to handle metadata changes
     */
    public void updateMetadata(Map<String, Object> newMetadata) {
        if (newMetadata == null || newMetadata.isEmpty()) {
            return;
        }

        // Merge metadata
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }

        this.metadata.putAll(newMetadata);

        //  Extract last_seen_at from metadata
        if (newMetadata.containsKey("last_seen_at")) {
            Object lastSeenObj = newMetadata.get("last_seen_at");
            if (lastSeenObj != null) {
                this.lastSeenAt = parseTimestamp(lastSeenObj);
            }
        }

        //  Extract first_seen_at from metadata
        if (newMetadata.containsKey("first_seen_at")) {
            Object firstSeenObj = newMetadata.get("first_seen_at");
            if (firstSeenObj != null) {
                Instant newFirstSeen = parseTimestamp(firstSeenObj);

                // Only update if earlier than current
                if (this.firstSeenAt == null || newFirstSeen.isBefore(this.firstSeenAt)) {
                    this.firstSeenAt = newFirstSeen;
                }
            }
        }
    }

    /**
     * Helper: Parse timestamp from various formats
     */
    private Instant parseTimestamp(Object obj) {
        if (obj instanceof Instant) {
            return (Instant) obj;
        }

        if (obj instanceof String) {
            try {
                return Instant.parse((String) obj);
            } catch (Exception e) {
                log.warn("Failed to parse timestamp: {}", obj);
                return Instant.now();
            }
        }

        if (obj instanceof Long) {
            return Instant.ofEpochMilli((Long) obj);
        }

        return Instant.now();
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
        Instant firstSeenAt = extractTimestampFromMetadata(metadata, "first_seen_at", now);
        Instant lastSeenAt = extractTimestampFromMetadata(metadata, "last_seen_at", now);

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
                .firstSeenAt(firstSeenAt)
                .lastSeenAt(lastSeenAt)
                .version(1)
                .build();
    }

    private static Instant extractTimestampFromMetadata(
            Map<String, Object> metadata,
            String key,
            Instant defaultValue) {

        if (metadata == null || !metadata.containsKey(key)) {
            return defaultValue;
        }

        Object value = metadata.get(key);

        if (value instanceof Instant) {
            return (Instant) value;
        }

        if (value instanceof String) {
            try {
                return Instant.parse((String) value);
            } catch (Exception e) {
                log.warn("Failed to parse {} from metadata: {}", key, value);
                return defaultValue;
            }
        }

        if (value instanceof Long) {
            return Instant.ofEpochMilli((Long) value);
        }

        return defaultValue;
    }

    /**
     * Check if two Traits objects are equal
     */
    private boolean traitsEquals(Traits t1, Traits t2) {
        if (t1 == null && t2 == null) return true;
        if (t1 == null || t2 == null) return false;

        return Objects.equals(t1.getFullName(), t2.getFullName()) &&
                Objects.equals(t1.getFirstName(), t2.getFirstName()) &&
                Objects.equals(t1.getLastName(), t2.getLastName()) &&
                Objects.equals(t1.getIdcard(), t2.getIdcard()) &&
                Objects.equals(t1.getOldIdcard(), t2.getOldIdcard()) &&
                Objects.equals(t1.getPhone(), t2.getPhone()) &&
                Objects.equals(t1.getEmail(), t2.getEmail()) &&
                Objects.equals(t1.getGender(), t2.getGender()) &&
                Objects.equals(t1.getDob(), t2.getDob()) &&
                Objects.equals(t1.getAddress(), t2.getAddress()) &&
                Objects.equals(t1.getReligion(), t2.getReligion());
    }

    /**
     * Check if two Platforms objects are equal
     */
    private boolean platformsEquals(Platforms p1, Platforms p2) {
        if (p1 == null && p2 == null) return true;
        if (p1 == null || p2 == null) return false;

        return Objects.equals(p1.getOs(), p2.getOs()) &&
                Objects.equals(p1.getDevice(), p2.getDevice()) &&
                Objects.equals(p1.getBrowser(), p2.getBrowser()) &&
                Objects.equals(p1.getAppVersion(), p2.getAppVersion());
    }

    /**
     * Check if two Campaign objects are equal
     */
    private boolean campaignEquals(Campaign c1, Campaign c2) {
        if (c1 == null && c2 == null) return true;
        if (c1 == null || c2 == null) return false;

        return Objects.equals(c1.getUtmSource(), c2.getUtmSource()) &&
                Objects.equals(c1.getUtmCampaign(), c2.getUtmCampaign()) &&
                Objects.equals(c1.getUtmMedium(), c2.getUtmMedium()) &&
                Objects.equals(c1.getUtmContent(), c2.getUtmContent()) &&
                Objects.equals(c1.getUtmTerm(), c2.getUtmTerm()) &&
                Objects.equals(c1.getUtmCustom(), c2.getUtmCustom());
    }
}