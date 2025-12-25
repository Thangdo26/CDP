package com.vft.cdp.profile.infra.es;

import com.vft.cdp.profile.domain.model.EnrichedProfile;
import com.vft.cdp.profile.domain.model.RawProfile;

/**
 * Mapper giữa EnrichedProfile (domain) và ProfileDocument (Elasticsearch)
 */
public final class ProfileMapper {

    private ProfileMapper() {
        throw new AssertionError("Utility class");
    }

    /**
     * Build ES document ID
     * Format: "{tenant_id}|{app_id}|{user_id}"
     */
    public static String buildId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    /**
     * ========= Domain → Elasticsearch =========
     */
    public static ProfileDocument toDocument(EnrichedProfile profile) {
        if (profile == null) {
            return null;
        }

        ProfileDocument doc = new ProfileDocument();

        String id = buildId(
                profile.getTenantId(),
                profile.getAppId(),
                profile.getUserId()
        );

        // ===== Identity =====
        doc.setId(id);                  // ES _id
        doc.setProfileId(id);          // business id

        doc.setTenantId(profile.getTenantId());
        doc.setAppId(profile.getAppId());
        doc.setUserId(profile.getUserId());
        doc.setType(profile.getType());

        // ===== Nested objects =====
        doc.setTraits(mapTraitsToDoc(profile.getTraits()));
        doc.setPlatforms(mapPlatformsToDoc(profile.getPlatforms()));
        doc.setCampaign(mapCampaignToDoc(profile.getCampaign()));
        doc.setMetadata(profile.getMetadata());

        // ===== System metadata =====
        doc.setPartitionKey(profile.getPartitionKey());
        doc.setEnrichedAt(profile.getEnrichedAt());
        doc.setEnrichedId(profile.getEnrichedId());

        // ===== Tracking timestamps =====
        doc.setCreatedAt(profile.getCreatedAt());
        doc.setUpdatedAt(profile.getUpdatedAt());
        doc.setFirstSeenAt(profile.getFirstSeenAt());
        doc.setLastSeenAt(profile.getLastSeenAt());
        doc.setVersion(profile.getVersion());

        return doc;
    }

    /**
     * ========= Elasticsearch → Domain =========
     */
    public static EnrichedProfile toDomain(ProfileDocument doc) {
        if (doc == null) {
            return null;
        }

        return EnrichedProfile.builder()
                .tenantId(doc.getTenantId())
                .appId(doc.getAppId())
                .userId(doc.getUserId())
                .type(doc.getType())

                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .metadata(doc.getMetadata())

                .partitionKey(doc.getPartitionKey())
                .enrichedAt(doc.getEnrichedAt())
                .enrichedId(doc.getEnrichedId())

                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .firstSeenAt(doc.getFirstSeenAt())
                .lastSeenAt(doc.getLastSeenAt())
                .version(doc.getVersion())
                .build();
    }

    // ================== Traits ==================

    private static ProfileDocument.TraitsDocument mapTraitsToDoc(RawProfile.Traits traits) {
        if (traits == null) {
            return null;
        }

        ProfileDocument.TraitsDocument doc = new ProfileDocument.TraitsDocument();
        doc.setFullName(traits.getFullName());
        doc.setFirstName(traits.getFirstName());
        doc.setLastName(traits.getLastName());
        doc.setIdcard(traits.getIdcard());
        doc.setOldIdcard(traits.getOldIdcard());
        doc.setPhone(traits.getPhone());
        doc.setEmail(traits.getEmail());
        doc.setGender(traits.getGender());
        doc.setDob(traits.getDob());
        doc.setAddress(traits.getAddress());
        doc.setReligion(traits.getReligion());
        return doc;
    }

    private static RawProfile.Traits mapTraitsToDomain(ProfileDocument.TraitsDocument doc) {
        if (doc == null) {
            return null;
        }

        return RawProfile.Traits.builder()
                .fullName(doc.getFullName())
                .firstName(doc.getFirstName())
                .lastName(doc.getLastName())
                .idcard(doc.getIdcard())
                .oldIdcard(doc.getOldIdcard())
                .phone(doc.getPhone())
                .email(doc.getEmail())
                .gender(doc.getGender())
                .dob(doc.getDob())
                .address(doc.getAddress())
                .religion(doc.getReligion())
                .build();
    }

    // ================== Platforms ==================

    private static ProfileDocument.PlatformsDocument mapPlatformsToDoc(RawProfile.Platforms platforms) {
        if (platforms == null) {
            return null;
        }

        ProfileDocument.PlatformsDocument doc = new ProfileDocument.PlatformsDocument();
        doc.setOs(platforms.getOs());
        doc.setDevice(platforms.getDevice());
        doc.setBrowser(platforms.getBrowser());
        doc.setAppVersion(platforms.getAppVersion());
        return doc;
    }

    private static RawProfile.Platforms mapPlatformsToDomain(ProfileDocument.PlatformsDocument doc) {
        if (doc == null) {
            return null;
        }

        return RawProfile.Platforms.builder()
                .os(doc.getOs())
                .device(doc.getDevice())
                .browser(doc.getBrowser())
                .appVersion(doc.getAppVersion())
                .build();
    }

    // ================== Campaign ==================

    private static ProfileDocument.CampaignDocument mapCampaignToDoc(RawProfile.Campaign campaign) {
        if (campaign == null) {
            return null;
        }

        ProfileDocument.CampaignDocument doc = new ProfileDocument.CampaignDocument();
        doc.setUtmSource(campaign.getUtmSource());
        doc.setUtmCampaign(campaign.getUtmCampaign());
        doc.setUtmMedium(campaign.getUtmMedium());
        doc.setUtmContent(campaign.getUtmContent());
        doc.setUtmTerm(campaign.getUtmTerm());
        doc.setUtmCustom(campaign.getUtmCustom());
        return doc;
    }

    private static RawProfile.Campaign mapCampaignToDomain(ProfileDocument.CampaignDocument doc) {
        if (doc == null) {
            return null;
        }

        return RawProfile.Campaign.builder()
                .utmSource(doc.getUtmSource())
                .utmCampaign(doc.getUtmCampaign())
                .utmMedium(doc.getUtmMedium())
                .utmContent(doc.getUtmContent())
                .utmTerm(doc.getUtmTerm())
                .utmCustom(doc.getUtmCustom())
                .build();
    }
}
