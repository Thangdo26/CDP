package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MAPPER - UTILITY CLASS (NO ENRICHED PROFILE)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ Only Domain Profile conversions
 * ✅ No dependency on cdp-common EnrichedProfile
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileMapper {

    private ProfileMapper() {
        throw new AssertionError("Utility class - no instantiation");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ID BUILDER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Build composite profile ID
     * Format: tenant_id|app_id|user_id
     */
    public static String buildId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOCUMENT → DOMAIN
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert ProfileDocument to Domain Profile
     */
    public static Profile toDomain(ProfileDocument doc) {
        if (doc == null) return null;

        return Profile.builder()
                .tenantId(doc.getTenantId())
                .appId(doc.getAppId())
                .userId(doc.getUserId())
                .type(doc.getType())
                .status(com.vft.cdp.profile.domain.ProfileStatus.fromValue(doc.getStatus()))
                .mergedToMasterId(doc.getMergedToMasterId())
                .mergedAt(doc.getMergedAt())
                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .metadata(doc.getMetadata())
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .firstSeenAt(doc.getFirstSeenAt())
                .lastSeenAt(doc.getLastSeenAt())
                .version(doc.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static Profile.Traits mapTraitsToDomain(ProfileDocument.Traits traits) {
        if (traits == null) return null;

        return Profile.Traits.builder()
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .phone(traits.getPhone())
                .email(traits.getEmail())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .religion(traits.getReligion())
                .build();
    }

    private static Profile.Platforms mapPlatformsToDomain(ProfileDocument.Platforms platforms) {
        if (platforms == null) return null;

        return Profile.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static Profile.Campaign mapCampaignToDomain(ProfileDocument.Campaign campaign) {
        if (campaign == null) return null;

        return Profile.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }
}