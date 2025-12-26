package com.vft.cdp.profile.domain;

import com.vft.cdp.profile.application.model.MasterProfileModel;
import com.vft.cdp.profile.application.model.ProfileModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE - DOMAIN ENTITY
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Domain Layer - Pure business entity
 * Implements MasterProfileModel interface from Application layer
 *
 * PURPOSE: Represents the consolidated master profile that contains
 * merged data from multiple individual profiles
 *
 * BUSINESS RULES:
 * - Master profile consolidates data from merged profiles
 * - Tracks which profiles have been merged into it
 * - Maintains consolidated traits, platforms, campaign data
 * - Can add new merged profiles
 * - Can update consolidated data
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MasterProfile implements MasterProfileModel {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // IDENTITY - Matching MasterProfileModel interface
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String tenantId;

    @Builder.Default
    private List<String> appId = new ArrayList<>();

    private String profileId; // This is the masterId

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATUS - Matching MasterProfileModel interface
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Builder.Default
    private String status = "ACTIVE";

    @Builder.Default
    private boolean isAnonymous = false;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MERGED PROFILES TRACKING
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Builder.Default
    private List<String> mergedIds = new ArrayList<>();

    @Builder.Default
    private List<String> deviceId = new ArrayList<>();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CONSOLIDATED DATA
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private MasterTraits traits;

    @Builder.Default
    private List<String> segments = new ArrayList<>();

    @Builder.Default
    private Map<String, Double> scores = new HashMap<>();

    @Builder.Default
    private Map<String, Consent> consents = new HashMap<>();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TIMESTAMPS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Instant createdAt;
    private Instant updatedAt;
    private Instant firstSeenAt;
    private Instant lastSeenAt;

    @Builder.Default
    private List<String> sourceSystems = new ArrayList<>();

    @Builder.Default
    private Integer version = 1;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BUSINESS METHODS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Add a profile to merged profiles list
     */
    public void addMergedProfile(String profileId) {
        if (this.mergedIds == null) {
            this.mergedIds = new ArrayList<>();
        }

        if (!this.mergedIds.contains(profileId)) {
            this.mergedIds.add(profileId);
            this.updatedAt = Instant.now();
        }
    }

    /**
     * Update consolidated data from a profile
     */
    public void updateFromProfile(ProfileModel profile) {
        if (profile == null) return;

        // Merge traits
        if (this.traits == null) {
            this.traits = new MasterTraits();
        }
        this.traits = mergeTraits(this.traits, profile.getTraits());

        // Update timestamps
        this.updatedAt = Instant.now();
        this.lastSeenAt = Instant.now();

        if (this.firstSeenAt == null ||
                (profile.getFirstSeenAt() != null && profile.getFirstSeenAt().isBefore(this.firstSeenAt))) {
            this.firstSeenAt = profile.getFirstSeenAt();
        }

        // Add app ID if not already present
        String profileAppId = profile.getAppId();
        if (profileAppId != null && !this.appId.contains(profileAppId)) {
            this.appId.add(profileAppId);
        }
    }

    /**
     * Merge traits - prefer non-null values and aggregate lists
     */
    private MasterTraits mergeTraits(MasterTraits existing, ProfileModel.TraitsModel incoming) {
        if (incoming == null) return existing;

        MasterTraits result = new MasterTraits();

        // Aggregate email addresses
        result.setEmail(new ArrayList<>(existing.getEmail() != null ? existing.getEmail() : new ArrayList<>()));
        if (incoming.getEmail() != null && !result.getEmail().contains(incoming.getEmail())) {
            result.getEmail().add(incoming.getEmail());
        }

        // Aggregate phone numbers
        result.setPhone(new ArrayList<>(existing.getPhone() != null ? existing.getPhone() : new ArrayList<>()));
        if (incoming.getPhone() != null && !result.getPhone().contains(incoming.getPhone())) {
            result.getPhone().add(incoming.getPhone());
        }

        // Aggregate user IDs
        result.setUserId(new ArrayList<>(existing.getUserId() != null ? existing.getUserId() : new ArrayList<>()));
        String incomingUserId = incoming.getIdcard(); // Assuming idcard maps to userId
        if (incomingUserId != null && !result.getUserId().contains(incomingUserId)) {
            result.getUserId().add(incomingUserId);
        }

        // Single value fields - prefer incoming if not null
        result.setFirstName(incoming.getFirstName() != null ? incoming.getFirstName() : existing.getFirstName());
        result.setLastName(incoming.getLastName() != null ? incoming.getLastName() : existing.getLastName());
        result.setGender(incoming.getGender() != null ? incoming.getGender() : existing.getGender());
        result.setDob(incoming.getDob() != null ? incoming.getDob() : existing.getDob());
        result.setCountry(existing.getCountry()); // Keep existing country
        result.setCity(existing.getCity()); // Keep existing city
        result.setAddress(incoming.getAddress() != null ? incoming.getAddress() : existing.getAddress());
        result.setLastPurchaseAmount(existing.getLastPurchaseAmount());
        result.setLastPurchaseAt(existing.getLastPurchaseAt());

        return result;
    }

    /**
     * Factory method to create master profile from first profile
     */
    public static MasterProfile createFromProfile(ProfileModel profile, String masterId) {
        Instant now = Instant.now();

        MasterProfile master = MasterProfile.builder()
                .tenantId(profile.getTenantId())
                .appId(new ArrayList<>(List.of(profile.getAppId())))
                .profileId(masterId)
                .mergedIds(new ArrayList<>(List.of(buildProfileId(profile))))
                .deviceId(new ArrayList<>())
                .traits(convertTraits(profile.getTraits()))
                .segments(new ArrayList<>())
                .scores(new HashMap<>())
                .consents(new HashMap<>())
                .createdAt(now)
                .updatedAt(now)
                .firstSeenAt(profile.getFirstSeenAt())
                .lastSeenAt(profile.getLastSeenAt())
                .sourceSystems(new ArrayList<>())
                .status("ACTIVE")
                .isAnonymous(profile.getTraits() == null ||
                        (profile.getTraits().getEmail() == null &&
                                profile.getTraits().getPhone() == null))
                .version(1)
                .build();

        return master;
    }

    private static String buildProfileId(ProfileModel profile) {
        return profile.getTenantId() + "|" + profile.getAppId() + "|" + profile.getUserId();
    }

    private static MasterTraits convertTraits(ProfileModel.TraitsModel source) {
        if (source == null) return new MasterTraits();

        MasterTraits traits = new MasterTraits();

        // Convert single values to lists
        traits.setEmail(source.getEmail() != null ? new ArrayList<>(List.of(source.getEmail())) : new ArrayList<>());
        traits.setPhone(source.getPhone() != null ? new ArrayList<>(List.of(source.getPhone())) : new ArrayList<>());
        traits.setUserId(source.getIdcard() != null ? new ArrayList<>(List.of(source.getIdcard())) : new ArrayList<>());

        // Copy single value fields
        traits.setFirstName(source.getFirstName());
        traits.setLastName(source.getLastName());
        traits.setGender(source.getGender());
        traits.setDob(source.getDob());
        traits.setAddress(source.getAddress());

        return traits;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // INNER CLASSES - Implement MasterProfileModel interfaces
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MasterTraits implements MasterProfileModel.MasterTraitsModel {
        @Builder.Default
        private List<String> email = new ArrayList<>();

        @Builder.Default
        private List<String> phone = new ArrayList<>();

        @Builder.Default
        private List<String> userId = new ArrayList<>();

        private String firstName;
        private String lastName;
        private String gender;
        private String dob;
        private String country;
        private String city;
        private String address;
        private Double lastPurchaseAmount;
        private Instant lastPurchaseAt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Consent implements MasterProfileModel.ConsentModel {
        private String status;
        private Instant updatedAt;
        private String source;
    }
}