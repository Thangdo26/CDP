package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE INFRASTRUCTURE MAPPER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Maps between:
 * - Profile (Domain) ↔ ProfileDocument (ES)
 * - ProfileModel (Interface) ↔ ProfileDocument (ES)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Component
public class ProfileInfraMapper {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOMAIN → DOCUMENT (for saving)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert Domain Profile to ES Document
     */
    public ProfileDocument toDocument(Profile profile) {
        if (profile == null) return null;

        String id = buildId(profile.getTenantId(), profile.getAppId(), profile.getUserId());

        return ProfileDocument.builder()
                .id(id)
                .tenantId(profile.getTenantId())
                .appId(profile.getAppId())
                .userId(profile.getUserId())
                .type(profile.getType())
                .status(profile.getStatus() != null ? profile.getStatus().getValue() : "active")
                .mergedToMasterId(profile.getMergedToMasterId())
                .mergedAt(profile.getMergedAt())
                .traits(mapTraitsToDoc(profile.getTraits()))
                .platforms(mapPlatformsToDoc(profile.getPlatforms()))
                .campaign(mapCampaignToDoc(profile.getCampaign()))
                .metadata(profile.getMetadata())
                .createdAt(profile.getCreatedAt())
                .updatedAt(profile.getUpdatedAt())
                .firstSeenAt(profile.getFirstSeenAt())
                .lastSeenAt(profile.getLastSeenAt())
                .version(profile.getVersion())
                .build();
    }

    /**
     * Convert ProfileModel to ES Document
     * (Used when saving ProfileModel that's not a Profile entity)
     */
    public ProfileDocument toDocument(ProfileModel model) {
        if (model == null) return null;

        // If it's already a Profile, use the specific method
        if (model instanceof Profile) {
            return toDocument((Profile) model);
        }

        // Otherwise, reconstruct from model interface
        String id = buildId(model.getTenantId(), model.getAppId(), model.getUserId());

        return ProfileDocument.builder()
                .id(id)
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())
                .status(model.getStatus())
                .mergedToMasterId(model.getMergedToMasterId())
                .mergedAt(model.getMergedAt())
                .traits(mapTraitsToDoc(model.getTraits()))
                .platforms(mapPlatformsToDoc(model.getPlatforms()))
                .campaign(mapCampaignToDoc(model.getCampaign()))
                .metadata(model.getMetadata())
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .version(model.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOCUMENT → DOMAIN (for business logic)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert ES Document to Domain Profile
     * (When we need business logic)
     */
    public Profile toDomain(ProfileDocument doc) {
        if (doc == null) return null;

        return Profile.builder()
                .tenantId(doc.getTenantId())
                .appId(doc.getAppId())
                .userId(doc.getUserId())
                .type(doc.getType())
                .status(ProfileStatus.fromValue(doc.getStatus()))
                .mergedToMasterId(doc.getMergedToMasterId())
                .mergedAt(doc.getMergedAt())
                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .metadata(doc.getMetadata() != null ? doc.getMetadata() : new HashMap<>())
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .firstSeenAt(doc.getFirstSeenAt())
                .lastSeenAt(doc.getLastSeenAt())
                .version(doc.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TRAITS MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private ProfileDocument.Traits mapTraitsToDoc(Profile.Traits traits) {
        if (traits == null) return null;

        return ProfileDocument.Traits.builder()
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

    private ProfileDocument.Traits mapTraitsToDoc(ProfileModel.TraitsModel traits) {
        if (traits == null) return null;

        // If already domain Traits, use specific method
        if (traits instanceof Profile.Traits) {
            return mapTraitsToDoc((Profile.Traits) traits);
        }

        return ProfileDocument.Traits.builder()
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

    private Profile.Traits mapTraitsToDomain(ProfileDocument.Traits doc) {
        if (doc == null) return null;

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
    // PLATFORMS MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private ProfileDocument.Platforms mapPlatformsToDoc(Profile.Platforms platforms) {
        if (platforms == null) return null;

        return ProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private ProfileDocument.Platforms mapPlatformsToDoc(ProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;

        if (platforms instanceof Profile.Platforms) {
            return mapPlatformsToDoc((Profile.Platforms) platforms);
        }

        return ProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private Profile.Platforms mapPlatformsToDomain(ProfileDocument.Platforms doc) {
        if (doc == null) return null;

        return Profile.Platforms.builder()
                .os(doc.getOs())
                .device(doc.getDevice())
                .browser(doc.getBrowser())
                .appVersion(doc.getAppVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CAMPAIGN MAPPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private ProfileDocument.Campaign mapCampaignToDoc(Profile.Campaign campaign) {
        if (campaign == null) return null;

        return ProfileDocument.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private ProfileDocument.Campaign mapCampaignToDoc(ProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;

        if (campaign instanceof Profile.Campaign) {
            return mapCampaignToDoc((Profile.Campaign) campaign);
        }

        return ProfileDocument.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private Profile.Campaign mapCampaignToDomain(ProfileDocument.Campaign doc) {
        if (doc == null) return null;

        return Profile.Campaign.builder()
                .utmSource(doc.getUtmSource())
                .utmCampaign(doc.getUtmCampaign())
                .utmMedium(doc.getUtmMedium())
                .utmContent(doc.getUtmContent())
                .utmTerm(doc.getUtmTerm())
                .utmCustom(doc.getUtmCustom())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UTILITIES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public String buildId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }
}