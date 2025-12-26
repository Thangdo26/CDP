package com.vft.cdp.profile.application.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE MODEL (INTERFACE)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface MasterProfileModel {

    // Identity
    String getProfileId();
    String getTenantId();
    List<String> getAppId();

    // Status
    String getStatus();
    Boolean isAnonymous();

    // Aggregated data
    List<String> getDeviceId();
    List<String> getMergedIds();
    MasterTraitsModel getTraits();
    List<String> getSegments();
    Map<String, Double> getScores();
    Map<String, ConsentModel> getConsents();

    // Metadata
    Instant getCreatedAt();
    Instant getUpdatedAt();
    Instant getFirstSeenAt();
    Instant getLastSeenAt();
    List<String> getSourceSystems();
    Integer getVersion();

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED INTERFACES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    interface MasterTraitsModel {
        List<String> getEmail();
        List<String> getPhone();
        List<String> getUserId();
        String getFirstName();
        String getLastName();
        String getGender();
        String getDob();
        String getCountry();
        String getCity();
        String getAddress();
        Double getLastPurchaseAmount();
        Instant getLastPurchaseAt();
    }

    interface ConsentModel {
        String getStatus();
        Instant getUpdatedAt();
        String getSource();
    }
}