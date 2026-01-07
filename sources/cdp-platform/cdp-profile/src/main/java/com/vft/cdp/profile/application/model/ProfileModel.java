package com.vft.cdp.profile.application.model;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MODEL (READ-ONLY INTERFACE)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface ProfileModel {

    // Identity
    String getTenantId();
    String getAppId();
    String getUserId();
    String getType();

    // Status
    String getStatus();
    String getMergedToMasterId();
    Instant getMergedAt();

    // Data
    TraitsModel getTraits();
    PlatformsModel getPlatforms();
    CampaignModel getCampaign();
    Map<String, Object> getMetadata();

    // Timestamps
    Instant getCreatedAt();
    Instant getUpdatedAt();
    Instant getFirstSeenAt();
    Instant getLastSeenAt();
    Integer getVersion();
    List< UserIdentityModel> getUsers();

    
    // NESTED INTERFACES

    interface UserIdentityModel {
        String getAppId();
        String getUserId();
    }

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

    interface PlatformsModel {
        String getOs();
        String getDevice();
        String getBrowser();
        String getAppVersion();
    }

    interface CampaignModel {
        String getUtmSource();
        String getUtmCampaign();
        String getUtmMedium();
        String getUtmContent();
        String getUtmTerm();
        String getUtmCustom();
    }
}