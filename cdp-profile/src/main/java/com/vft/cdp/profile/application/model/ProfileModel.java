package com.vft.cdp.profile.application.model;

import java.time.Instant;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MODEL (READ-ONLY INTERFACE)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * CONTRACT between Application and Infrastructure layers.
 *
 * PURPOSE:
 * - Application layer works ONLY with this interface
 * - Domain entities implement this interface
 * - Infrastructure adapters implement this interface
 *
 * BENEFITS:
 * - Application doesn't depend on Domain entities
 * - Application doesn't depend on Infrastructure documents
 * - Can change database (ES → Mongo) without changing Application
 * - Easy to mock for testing
 *
 * PATTERN: Hexagonal Architecture / Ports & Adapters
 *
 * IMPLEMENTATIONS:
 * - Profile.java (Domain layer) - Rich business logic
 * - ProfileModelImpl.java (Infrastructure) - Lightweight adapter
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface ProfileModel {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // IDENTITY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    String getTenantId();

    String getAppId();

    String getUserId();

    String getType();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get profile status: "active" | "merged" | "deleted"
     */
    String getStatus();

    /**
     * Get master profile ID if this profile was merged
     */
    String getMergedToMasterId();

    /**
     * Get timestamp when this profile was merged
     */
    Instant getMergedAt();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DATA
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get user traits (personal information)
     */
    TraitsModel getTraits();

    /**
     * Get platform/device information
     */
    PlatformsModel getPlatforms();

    /**
     * Get campaign/UTM information
     */
    CampaignModel getCampaign();

    /**
     * Get additional metadata
     */
    Map<String, Object> getMetadata();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TIMESTAMPS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Instant getCreatedAt();

    Instant getUpdatedAt();

    Instant getFirstSeenAt();

    Instant getLastSeenAt();

    Integer getVersion();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED MODEL INTERFACES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * User traits (personal information)
     */
    interface TraitsModel {
        String getFullName();
        String getFirstName();
        String getLastName();
        String getIdcard();
        String getOldIdcard();
        String getPhone();
        String getEmail();
        String getGender();
        String getDob();
        String getAddress();
        String getReligion();
    }

    /**
     * Device/Platform information
     */
    interface PlatformsModel {
        String getOs();
        String getDevice();
        String getBrowser();
        String getAppVersion();
    }

    /**
     * Campaign/UTM information
     */
    interface CampaignModel {
        String getUtmSource();
        String getUtmCampaign();
        String getUtmMedium();
        String getUtmContent();
        String getUtmTerm();
        String getUtmCustom();
    }
}