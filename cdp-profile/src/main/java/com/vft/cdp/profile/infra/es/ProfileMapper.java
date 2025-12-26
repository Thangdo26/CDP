package com.vft.cdp.profile.infra.es;

import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MAPPER - DDD PATTERN
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Maps between Domain Model and Infrastructure Model:
 * - Profile (domain) ↔ ProfileDocument (infra)
 *
 * RESPONSIBILITY:
 * - Convert domain entities to ES documents for persistence
 * - Convert ES documents back to domain entities for business logic
 * - Handle status field mapping
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileMapper {

    private ProfileMapper() {
        throw new AssertionError("Utility class");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BUILD COMPOSITE ID
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Build ES document ID
     * Format: "{tenant_id}|{app_id}|{user_id}"
     */
    public static String buildId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOMAIN → INFRASTRUCTURE (for persistence)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert Domain Profile to ES ProfileDocument
     */
    public static ProfileDocument toDocument(Profile profile) {
        if (profile == null) {
            return null;
        }

        ProfileDocument doc = new ProfileDocument();

        // Build composite ID
        String id = buildId(
                profile.getTenantId(),
                profile.getAppId(),
                profile.getUserId()
        );

        // ===== IDENTITY =====
        doc.setId(id);
        doc.setProfileId(id);
        doc.setTenantId(profile.getTenantId());
        doc.setAppId(profile.getAppId());
        doc.setUserId(profile.getUserId());
        doc.setType(profile.getType());

        doc.setStatus(profile.getStatus() != null
                ? profile.getStatus().getValue()
                : ProfileStatus.ACTIVE.getValue());
        doc.setMergedToMasterId(profile.getMergedToMasterId());
        doc.setMergedAt(profile.getMergedAt());

        // ===== VALUE OBJECTS =====
        doc.setTraits(mapTraitsToDoc(profile.getTraits()));
        doc.setPlatforms(mapPlatformsToDoc(profile.getPlatforms()));
        doc.setCampaign(mapCampaignToDoc(profile.getCampaign()));
        doc.setMetadata(profile.getMetadata());

        // ===== SYSTEM METADATA =====
        doc.setPartitionKey(profile.getPartitionKey());
        doc.setEnrichedAt(profile.getEnrichedAt());
        doc.setEnrichedId(profile.getEnrichedId());

        // ===== TIMESTAMPS =====
        doc.setCreatedAt(profile.getCreatedAt());
        doc.setUpdatedAt(profile.getUpdatedAt());
        doc.setFirstSeenAt(profile.getFirstSeenAt());
        doc.setLastSeenAt(profile.getLastSeenAt());
        doc.setVersion(profile.getVersion());

        return doc;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // INFRASTRUCTURE → DOMAIN (for business logic)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert ES ProfileDocument to Domain Profile
     */
    public static Profile toDomain(ProfileDocument doc) {
        if (doc == null) {
            return null;
        }

        return Profile.builder()
                // ===== IDENTITY =====
                .profileId(Profile.ProfileId.builder()
                        .tenantId(doc.getTenantId())
                        .appId(doc.getAppId())
                        .userId(doc.getUserId())
                        .build())
                .type(doc.getType())

                // ===== ✅ STATUS (NEW) =====
                .status(ProfileStatus.fromValue(doc.getStatus()))
                .mergedToMasterId(doc.getMergedToMasterId())
                .mergedAt(doc.getMergedAt())

                // ===== VALUE OBJECTS =====
                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .metadata(doc.getMetadata())

                // ===== SYSTEM METADATA =====
                .partitionKey(doc.getPartitionKey())
                .enrichedAt(doc.getEnrichedAt())
                .enrichedId(doc.getEnrichedId())

                // ===== TIMESTAMPS =====
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .firstSeenAt(doc.getFirstSeenAt())
                .lastSeenAt(doc.getLastSeenAt())
                .version(doc.getVersion())

                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECT MAPPERS - TRAITS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static ProfileDocument.TraitsDocument mapTraitsToDoc(Profile.Traits traits) {
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

    private static Profile.Traits mapTraitsToDomain(ProfileDocument.TraitsDocument doc) {
        if (doc == null) {
            return null;
        }

        return Profile.Traits.builder()
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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECT MAPPERS - PLATFORMS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static ProfileDocument.PlatformsDocument mapPlatformsToDoc(Profile.Platforms platforms) {
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

    private static Profile.Platforms mapPlatformsToDomain(ProfileDocument.PlatformsDocument doc) {
        if (doc == null) {
            return null;
        }

        return Profile.Platforms.builder()
                .os(doc.getOs())
                .device(doc.getDevice())
                .browser(doc.getBrowser())
                .appVersion(doc.getAppVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // VALUE OBJECT MAPPERS - CAMPAIGN
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static ProfileDocument.CampaignDocument mapCampaignToDoc(Profile.Campaign campaign) {
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

    private static Profile.Campaign mapCampaignToDomain(ProfileDocument.CampaignDocument doc) {
        if (doc == null) {
            return null;
        }

        return Profile.Campaign.builder()
                .utmSource(doc.getUtmSource())
                .utmCampaign(doc.getUtmCampaign())
                .utmMedium(doc.getUtmMedium())
                .utmContent(doc.getUtmContent())
                .utmTerm(doc.getUtmTerm())
                .utmCustom(doc.getUtmCustom())
                .build();
    }
}