package com.vft.cdp.profile.application.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE MODEL (INTERFACE)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * CONTRACT for Master Profile (unified customer profile).
 *
 * PURPOSE:
 * - Application layer works with this interface
 * - Domain MasterProfile entity implements this
 * - Infrastructure adapter implements this
 *
 * PATTERN: Hexagonal Architecture / Ports & Adapters
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface MasterProfileModel {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // IDENTITY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get master profile ID (e.g., "mp_uuid")
     */
    String getProfileId();

    /**
     * Get tenant ID
     */
    String getTenantId();

    /**
     * Get all app IDs this customer used
     */
    List<String> getAppId();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get master profile status
     */
    String getStatus();

    /**
     * Check if this is an anonymous profile
     */
    Boolean isAnonymous();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // AGGREGATED DATA
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get all device IDs
     */
    List<String> getDeviceId();

    /**
     * Get IDs of profiles that were merged into this master
     */
    List<String> getMergedIds();

    /**
     * Get aggregated traits
     */
    MasterTraitsModel getTraits();

    /**
     * Get segment IDs
     */
    List<String> getSegments();

    /**
     * Get AI scores
     */
    Map<String, Double> getScores();

    /**
     * Get consent information
     */
    Map<String, ConsentModel> getConsents();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // METADATA
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    Instant getCreatedAt();

    Instant getUpdatedAt();

    Instant getFirstSeenAt();

    Instant getLastSeenAt();

    /**
     * Get source systems (web, app, api, etc.)
     */
    List<String> getSourceSystems();

    Integer getVersion();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED MODEL INTERFACES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Aggregated traits from all merged profiles
     */
    interface MasterTraitsModel {
        // Aggregated (lists)
        List<String> getEmail();
        List<String> getPhone();
        List<String> getUserId();

        // Single values (from latest profile)
        String getFirstName();
        String getLastName();
        String getGender();
        String getDob();
        String getCountry();
        String getCity();
        String getAddress();

        // Business metrics
        Double getLastPurchaseAmount();
        Instant getLastPurchaseAt();
    }

    /**
     * Consent information
     */
    interface ConsentModel {
        /**
         * Consent status: granted | denied | pending
         */
        String getStatus();

        Instant getUpdatedAt();

        /**
         * Source of consent: web_form | api | app
         */
        String getSource();
    }
}