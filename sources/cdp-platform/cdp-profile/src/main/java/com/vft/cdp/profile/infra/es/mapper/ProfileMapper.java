package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MAPPER - 100% CORRECT VERSION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * BASED ON YOUR ACTUAL Profile.java:
 * - Profile has: private ProfileStatus status (enum)
 * - Profile has: getStatus() returns String (already converted!)
 *
 * SO: profile.getStatus() → String (NO .getValue() needed!)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileMapper {

    private ProfileMapper() {
        throw new AssertionError("Utility class - no instantiation");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ID BUILDER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public static String buildId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOMAIN → DOCUMENT
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert Domain Profile to ProfileDocument
     *
     * CRITICAL: profile.getStatus() ALREADY returns String!
     */
    public static ProfileDocument toDocument(Profile profile) {
        if (profile == null) return null;

        String docId = buildId(profile.getTenantId(), profile.getAppId(), profile.getUserId());

        return ProfileDocument.builder()
                .id(docId)
                .tenantId(profile.getTenantId())
                .appId(profile.getAppId())
                .userId(profile.getUserId())
                .type(profile.getType())
                .status(profile.getStatus())  //  getStatus() returns String
                .mergedToMasterId(profile.getMergedToMasterId())
                .mergedAt(profile.getMergedAt())
                .traits(mapTraitsToDoc(profile.getTraits()))
                .platforms(mapPlatformsToDoc(profile.getPlatforms()))
                .campaign(mapCampaignToDoc(profile.getCampaign()))
                .metadata(profile.getMetadata())
                .createdAt(profile.getCreatedAt())
                .updatedAt(profile.getUpdatedAt())
                .version(profile.getVersion())
                .build();
    }

    /**
     * Convert ProfileModel interface to ProfileDocument
     */
    public static ProfileDocument toDocument(ProfileModel model) {
        if (model == null) return null;

        String docId = buildId(model.getTenantId(), model.getAppId(), model.getUserId());

        return ProfileDocument.builder()
                .id(docId)
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())
                .status(model.getStatus())  //  Also returns String
                .mergedToMasterId(model.getMergedToMasterId())
                .mergedAt(model.getMergedAt())
                .traits(mapTraitsToDoc(model.getTraits()))
                .platforms(mapPlatformsToDoc(model.getPlatforms()))
                .campaign(mapCampaignToDoc(model.getCampaign()))
                .metadata(model.getMetadata())
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .version(model.getVersion())
                .build();
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
                .status(ProfileStatus.fromValue(doc.getStatus()))  //  String → Enum
                .mergedToMasterId(doc.getMergedToMasterId())
                .mergedAt(doc.getMergedAt())
                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .metadata(doc.getMetadata())
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .version(doc.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER MAPPERS - Domain → Document
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static ProfileDocument.Traits mapTraitsToDoc(Profile.Traits traits) {
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

    private static ProfileDocument.Traits mapTraitsToDoc(ProfileModel.TraitsModel traits) {
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

    private static ProfileDocument.Platforms mapPlatformsToDoc(Profile.Platforms platforms) {
        if (platforms == null) return null;

        return ProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static ProfileDocument.Platforms mapPlatformsToDoc(ProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;

        return ProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static ProfileDocument.Campaign mapCampaignToDoc(Profile.Campaign campaign) {
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

    private static ProfileDocument.Campaign mapCampaignToDoc(ProfileModel.CampaignModel campaign) {
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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER MAPPERS - Document → Domain
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