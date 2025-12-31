package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE MAPPER - FIXED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * FIX 1: Copy ALL data from newest profile (platforms, campaign)
 * FIX 2: Correct method signatures for Profile interfaces
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class MasterProfileMapper {

    private MasterProfileMapper() {
        throw new AssertionError("Utility class");
    }


    // MERGE PROFILES → MASTER PROFILE


    public static MasterProfile mergeProfiles(List<Profile> profiles) {
        if (profiles == null || profiles.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty profile list");
        }


        // Sort profiles by last_seen_at DESC (newest first)


        List<Profile> sortedProfiles = profiles.stream()
                .sorted((p1, p2) -> {
                    Instant t1 = p1.getLastSeenAt();
                    Instant t2 = p2.getLastSeenAt();

                    if (t1 == null && t2 == null) return 0;
                    if (t1 == null) return 1;
                    if (t2 == null) return -1;

                    return t2.compareTo(t1);
                })
                .collect(Collectors.toList());

        // NEWEST profile (first after sort)
        Profile newestProfile = sortedProfiles.get(0);
        Profile firstProfile = profiles.get(0);

        Instant now = Instant.now();

        String masterProfileId = "mp_" + UUID.randomUUID().toString();

        List<String> mergedIds = profiles.stream()
                .map(p -> buildProfileId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        // FIX: Remove () from method reference
        List<String> appIds = profiles.stream()
                .map(Profile::getAppId)
                .distinct()
                .collect(Collectors.toList());

        // Merge traits with priority to newest profile
        MasterProfile.MasterTraits masterTraits = mergeTraitsWithPriority(sortedProfiles);

        // NEW: Copy platforms from NEWEST profile (using ProfileModel interface)
        MasterProfile.MasterPlatforms masterPlatforms =
                convertPlatformsFromModel(newestProfile.getPlatforms());

        // NEW: Copy campaign from NEWEST profile (using ProfileModel interface)
        MasterProfile.MasterCampaign masterCampaign =
                convertCampaignFromModel(newestProfile.getCampaign());


        // Calculate timestamps


        Instant firstSeenAt = profiles.stream()
                .map(Profile::getFirstSeenAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(now);

        Instant lastSeenAt = profiles.stream()
                .map(Profile::getLastSeenAt)
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(now);

        log.info("📦 Creating master from {} profiles (newest: {})",
                profiles.size(), newestProfile.getUserId());

        return MasterProfile.builder()
                .profileId(masterProfileId)
                .tenantId(firstProfile.getTenantId())
                .appId(appIds)
                .status("ACTIVE")
                .isAnonymous(isAnonymous(masterTraits))
                .deviceId(new ArrayList<>())
                .mergedIds(mergedIds)
                .traits(masterTraits)
                .platforms(masterPlatforms)      // NEW
                .campaign(masterCampaign)        // NEW
                .segments(new ArrayList<>())
                .scores(new HashMap<>())
                .consents(new HashMap<>())
                .createdAt(now)
                .updatedAt(now)
                .firstSeenAt(firstSeenAt)
                .lastSeenAt(lastSeenAt)
                .sourceSystems(new ArrayList<>())
                .version(1)
                .build();
    }


    // MERGE TRAITS FROM MULTIPLE PROFILES


    private static MasterProfile.MasterTraits mergeTraitsWithPriority(List<Profile> profiles) {

        // Aggregate emails
        List<String> emails = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getEmail())
                .filter(Objects::nonNull)
                .filter(e -> !e.isBlank())
                .distinct()
                .collect(Collectors.toList());

        // Aggregate phones
        List<String> phones = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getPhone())
                .filter(Objects::nonNull)
                .filter(p -> !p.isBlank())
                .distinct()
                .collect(Collectors.toList());

        // Aggregate user IDs
        List<String> userIds = profiles.stream()
                .map(Profile::getUserId)
                .filter(Objects::nonNull)
                .filter(id -> !id.isBlank())
                .distinct()
                .collect(Collectors.toList());


        // SINGLE VALUE FIELDS - FROM NEWEST (first in sorted list)


        String fullName = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getFullName())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        String firstName = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getFirstName())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        String lastName = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getLastName())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        String gender = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getGender())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        String dob = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getDob())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        String address = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getAddress())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        String idcard = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getIdcard())
                .filter(Objects::nonNull)
                .filter(id -> !id.isBlank())
                .findFirst()
                .orElse(null);

        String oldIdcard = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getOldIdcard())
                .filter(Objects::nonNull)
                .filter(id -> !id.isBlank())
                .findFirst()
                .orElse(null);

        String religion = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getReligion())
                .filter(Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);

        return MasterProfile.MasterTraits.builder()
                .email(emails)
                .phone(phones)
                .userId(userIds)
                .fullName(fullName)
                .firstName(firstName)
                .lastName(lastName)
                .gender(gender)
                .dob(dob)
                .address(address)
                .idcard(idcard)
                .oldIdcard(oldIdcard)
                .religion(religion)
                .build();
    }

    /**
     * NEW: Convert platforms from ProfileModel interface
     */
    private static MasterProfile.MasterPlatforms convertPlatformsFromModel(
            com.vft.cdp.profile.application.model.ProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;

        return MasterProfile.MasterPlatforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    /**
     * NEW: Convert campaign from ProfileModel interface
     */
    private static MasterProfile.MasterCampaign convertCampaignFromModel(
            com.vft.cdp.profile.application.model.ProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;

        return MasterProfile.MasterCampaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }


    // DOMAIN → DOCUMENT


    public static MasterProfileDocument toDocument(MasterProfile master) {
        if (master == null) return null;

        return MasterProfileDocument.builder()
                .id(master.getMasterId())
                .tenantId(master.getTenantId())
                .appId(master.getAppId().isEmpty() ? "default" : master.getAppId().get(0))
                .masterId(master.getProfileId())
                .status(master.getStatus())
                .mergedProfileIds(master.getMergedIds())
                .mergeCount(master.getMergedIds() != null ? master.getMergedIds().size() : 0)
                .traits(mapTraitsToDoc(master.getTraits()))
                .platforms(mapPlatformsToDoc(master.getPlatforms()))
                .campaign(mapCampaignToDoc(master.getCampaign()))
                .metadata(new HashMap<>())
                .createdAt(master.getCreatedAt())
                .updatedAt(master.getUpdatedAt())
                .firstSeenAt(master.getFirstSeenAt())
                .lastSeenAt(master.getLastSeenAt())
                .version(master.getVersion())
                .build();
    }


    // DOCUMENT → DOMAIN


    public static MasterProfile toDomain(MasterProfileDocument doc) {
        if (doc == null) return null;

        return MasterProfile.builder()
                .profileId(doc.getMasterId())
                .tenantId(doc.getTenantId())
                .appId(doc.getAppId() != null ? List.of(doc.getAppId()) : new ArrayList<>())
                .status(doc.getStatus() != null ? doc.getStatus() : "ACTIVE")
                .isAnonymous(false)
                .deviceId(new ArrayList<>())
                .mergedIds(doc.getMergedProfileIds() != null ? doc.getMergedProfileIds() : new ArrayList<>())
                .traits(mapTraitsToDomain(doc.getTraits()))
                .platforms(mapPlatformsToDomain(doc.getPlatforms()))
                .campaign(mapCampaignToDomain(doc.getCampaign()))
                .segments(new ArrayList<>())
                .scores(new HashMap<>())
                .consents(new HashMap<>())
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .firstSeenAt(doc.getFirstSeenAt())
                .lastSeenAt(doc.getLastSeenAt())
                .sourceSystems(new ArrayList<>())
                .version(doc.getVersion())
                .build();
    }


    // HELPER METHODS - Document Mapping


    private static MasterProfileDocument.Platforms mapPlatformsToDoc(
            com.vft.cdp.profile.application.model.MasterProfileModel.PlatformsModel platforms) {

        if (platforms == null) return null;

        return MasterProfileDocument.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static MasterProfileDocument.Campaign mapCampaignToDoc(
            com.vft.cdp.profile.application.model.MasterProfileModel.CampaignModel campaign) {

        if (campaign == null) return null;

        return MasterProfileDocument.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private static MasterProfileDocument.Traits mapTraitsToDoc(
            com.vft.cdp.profile.application.model.MasterProfileModel.MasterTraitsModel traits) {
        if (traits == null) return null;

        return MasterProfileDocument.Traits.builder()
                .email(traits.getEmail() != null ? new ArrayList<>(traits.getEmail()) : new ArrayList<>())
                .phone(traits.getPhone() != null ? new ArrayList<>(traits.getPhone()) : new ArrayList<>())
                .userId(traits.getUserId() != null ? new ArrayList<>(traits.getUserId()) : new ArrayList<>())
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .religion(traits.getReligion())
                .build();
    }

    private static MasterProfile.MasterTraits mapTraitsToDomain(MasterProfileDocument.Traits traits) {
        if (traits == null) return null;

        return MasterProfile.MasterTraits.builder()
                .email(traits.getEmail() != null ? new ArrayList<>(traits.getEmail()) : new ArrayList<>())
                .phone(traits.getPhone() != null ? new ArrayList<>(traits.getPhone()) : new ArrayList<>())
                .userId(traits.getUserId() != null ? new ArrayList<>(traits.getUserId()) : new ArrayList<>())
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .religion(traits.getReligion())
                .build();
    }

    /**
     * NEW: Map platforms from document to domain
     */
    private static MasterProfile.MasterPlatforms mapPlatformsToDomain(
            MasterProfileDocument.Platforms platforms) {
        if (platforms == null) return null;

        return MasterProfile.MasterPlatforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    /**
     * NEW: Map campaign from document to domain
     */
    private static MasterProfile.MasterCampaign mapCampaignToDomain(
            MasterProfileDocument.Campaign campaign) {
        if (campaign == null) return null;

        return MasterProfile.MasterCampaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    private static boolean isAnonymous(MasterProfile.MasterTraits traits) {
        return traits == null ||
                (traits.getEmail().isEmpty() && traits.getPhone().isEmpty());
    }

    private static String buildProfileId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }
}