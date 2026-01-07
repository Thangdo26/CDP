package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MAPPER - UPDATED FOR NEW ARCHITECTURE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * NEW ARCHITECTURE:
 * - ProfileDocument: Only stores profile data (NO tenant/app/user)
 * - ProfileMappingDocument: Maps (tenant, app, user) → profile_id
 * - Profile (Domain): Still has tenant/app/user for business logic
 *
 * IMPORTANT:
 * - When converting Domain → Document: tenant/app/user are NOT stored
 * - When converting Document → Domain: tenant/app/user will be null
 *   (must be set separately from ProfileMapping)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class ProfileMapper {

    private ProfileMapper() {
        throw new AssertionError("Utility class - no instantiation");
    }

    // ═══════════════════════════════════════════════════════════════
    // ID BUILDERS
    // ═══════════════════════════════════════════════════════════════

    /**
     * Build mapping ID: tenant_id|app_id|user_id
     * Used for ProfileMappingDocument ID
     */
    public static String buildMappingId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    public static String buildProfileId(Profile profile) {
        return profile.getUserId();
    }

    /**
     * Build profile ID from ProfileModel
     */
    public static String buildProfileId(ProfileModel model) {
        return model.getUserId();
    }

    public static boolean isUuidBasedId(String id) {
        return id != null && id.startsWith("uuid:");
    }

    // ═══════════════════════════════════════════════════════════════
    // DOMAIN → DOCUMENT
    // ═══════════════════════════════════════════════════════════════

    /**
     * Convert Domain Profile to ProfileDocument
     *
     * NOTE: tenantId, appId, userId are NOT stored in ProfileDocument
     * They are stored in ProfileMappingDocument separately
     */
    public static ProfileDocument toDocument(Profile profile) {
        if (profile == null) return null;

        String docId = buildProfileId(profile);

        return ProfileDocument.builder()
                .id(docId)
                .tenantId(profile.getTenantId())
                .type(profile.getType())
                .status(profile.getStatus())
                .users(mapUsersToDoc(profile.getUsers()))
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
     * Convert Domain Profile to ProfileDocument with specific ID
     */
    public static ProfileDocument toDocumentWithId(Profile profile, String documentId) {
        if (profile == null) return null;

        return ProfileDocument.builder()
                .id(documentId)
                .type(profile.getType())
                .status(profile.getStatus())
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
     * Convert ProfileModel interface to ProfileDocument
     */
    public static ProfileDocument toDocument(ProfileModel model) {
        if (model == null) return null;

        // Determine document ID
        String docId = buildProfileId(model);

        return ProfileDocument.builder()
                .id(docId)
                .type(model.getType())
                .status(model.getStatus())
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

    /**
     * Convert ProfileModel to ProfileDocument with specific ID
     */
    public static ProfileDocument toDocumentWithId(ProfileModel model, String documentId) {
        if (model == null) return null;

        return ProfileDocument.builder()
                .id(documentId)
                .type(model.getType())
                .status(model.getStatus())
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

    // ═══════════════════════════════════════════════════════════════
    // DOCUMENT → DOMAIN
    // ═══════════════════════════════════════════════════════════════

    /**
     * Convert ProfileDocument to Domain Profile
     *
     * ✅ FIXED: Now sets userId from document ID
     * NOTE: appId will be NULL, must be set separately if needed
     */
    public static Profile toDomain(ProfileDocument doc) {
        if (doc == null) return null;

        return Profile.builder()
                // ✅ FIXED: Use document ID as userId (this is the profile_id)
                .tenantId(doc.getTenantId())
                .userId(doc.getId())  // ← FIX: Set userId from document ID
                .type(doc.getType())
                .status(ProfileStatus.fromValue(doc.getStatus()))
                .users(mapUsersToDomain(doc.getUsers()))
                // mergedToMasterId and mergedAt are not in new ProfileDocument
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

    /**
     * Convert ProfileDocument to Domain Profile WITH identity info
     *
     * Use this when you have the mapping info available
     */
    public static Profile toDomain(ProfileDocument doc, String tenantId, String appId, String userId) {
        if (doc == null) return null;

        return Profile.builder()
                .tenantId(tenantId)
                .appId(appId)
                .userId(userId)
                .type(doc.getType())
                .status(ProfileStatus.fromValue(doc.getStatus()))
                .users(mapUsersToDomain(doc.getUsers()))
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

    // ═══════════════════════════════════════════════════════════════
    // HELPER MAPPERS - Domain → Document
    // ═══════════════════════════════════════════════════════════════

    private static List<ProfileDocument.UserIdentity> mapUsersToDoc(List<? extends ProfileModel.UserIdentityModel> users) {
        if (users == null) {
            return null;
        }

        return users.stream()
                .map(u -> ProfileDocument.UserIdentity.builder()
                        .appId(u.getAppId())
                        .userId(u.getUserId())
                        .build())
                .collect(Collectors.toList());
    }


    // ✅ NEW: Map users to domain
    private static List<Profile.UserIdentity> mapUsersToDomain(List<ProfileDocument.UserIdentity> users) {
        if (users == null) {
            return null;
        }

        return users.stream()
                .map(u -> Profile.UserIdentity.builder()
                        .appId(u.getAppId())
                        .userId(u.getUserId())
                        .build())
                .collect(Collectors.toList());
    }

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

    // ═══════════════════════════════════════════════════════════════
    // HELPER MAPPERS - Document → Domain
    // ═══════════════════════════════════════════════════════════════

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