package com.vft.cdp.profile.infra.es.mapper;

import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE MAPPER (NO CDP-COMMON DEPENDENCY)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ Uses Domain MasterProfile only
 * ✅ No dependency on cdp-common
 * ✅ Static utility methods
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public final class MasterProfileMapper {

    private MasterProfileMapper() {
        throw new AssertionError("Utility class");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MERGE PROFILES → MASTER PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Merge multiple Domain Profiles into a single MasterProfile
     */
    public static MasterProfile mergeProfiles(List<Profile> profiles) {
        if (profiles == null || profiles.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty profile list");
        }

        Profile firstProfile = profiles.get(0);
        Instant now = Instant.now();

        String masterProfileId = "mp_" + UUID.randomUUID().toString();

        List<String> mergedIds = profiles.stream()
                .map(p -> buildProfileId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        List<String> appIds = profiles.stream()
                .map(Profile::getAppId)
                .distinct()
                .collect(Collectors.toList());

        MasterProfile.MasterTraits masterTraits = mergeTraits(profiles);

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

        return MasterProfile.builder()
                .profileId(masterProfileId)
                .tenantId(firstProfile.getTenantId())
                .appId(appIds)
                .status("ACTIVE")
                .isAnonymous(isAnonymous(masterTraits))
                .deviceId(new ArrayList<>())
                .mergedIds(mergedIds)
                .traits(masterTraits)
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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MERGE TRAITS FROM MULTIPLE PROFILES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static MasterProfile.MasterTraits mergeTraits(List<Profile> profiles) {

        List<String> emails = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getEmail)
                .filter(Objects::nonNull)
                .map(String::toLowerCase)
                .distinct()
                .collect(Collectors.toList());

        List<String> phones = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getPhone)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        List<String> userIds = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getIdcard)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());

        String firstName = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getFirstName)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        String lastName = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getLastName)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        String gender = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getGender)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        String dob = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getDob)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        String address = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(Profile.Traits::getAddress)
                .filter(Objects::nonNull)
                .findFirst()
                .orElse(null);

        return MasterProfile.MasterTraits.builder()
                .email(emails)
                .phone(phones)
                .userId(userIds)
                .firstName(firstName)
                .lastName(lastName)
                .gender(gender)
                .dob(dob)
                .address(address)
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOMAIN → DOCUMENT
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public static MasterProfileDocument toDocument(MasterProfile master) {
        if (master == null) return null;

        String docId = buildMasterDocId(
                master.getTenantId(),
                master.getAppId().isEmpty() ? "default" : master.getAppId().get(0),
                master.getProfileId()
        );

        return MasterProfileDocument.builder()
                .id(docId)
                .tenantId(master.getTenantId())
                .appId(master.getAppId().isEmpty() ? "default" : master.getAppId().get(0))
                .masterId(master.getProfileId())
                .status(master.getStatus())
                .mergedProfileIds(master.getMergedIds())
                .mergeCount(master.getMergedIds() != null ? master.getMergedIds().size() : 0)
                .traits(mapTraitsToDoc(master.getTraits()))
                .platforms(null)
                .campaign(null)
                .metadata(new HashMap<>())
                .createdAt(master.getCreatedAt())
                .updatedAt(master.getUpdatedAt())
                .firstSeenAt(master.getFirstSeenAt())
                .lastSeenAt(master.getLastSeenAt())
                .version(master.getVersion())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DOCUMENT → DOMAIN
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER METHODS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private static MasterProfileDocument.Traits mapTraitsToDoc(MasterProfile.MasterTraits traits) {
        if (traits == null) return null;

        return MasterProfileDocument.Traits.builder()
                .email(traits.getEmail() != null && !traits.getEmail().isEmpty()
                        ? traits.getEmail().get(0) : null)
                .phone(traits.getPhone() != null && !traits.getPhone().isEmpty()
                        ? traits.getPhone().get(0) : null)
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .build();
    }

    private static MasterProfile.MasterTraits mapTraitsToDomain(MasterProfileDocument.Traits traits) {
        if (traits == null) return null;

        return MasterProfile.MasterTraits.builder()
                .email(traits.getEmail() != null ? List.of(traits.getEmail()) : new ArrayList<>())
                .phone(traits.getPhone() != null ? List.of(traits.getPhone()) : new ArrayList<>())
                .userId(new ArrayList<>())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .build();
    }

    private static boolean isAnonymous(MasterProfile.MasterTraits traits) {
        return traits == null ||
                (traits.getEmail().isEmpty() && traits.getPhone().isEmpty());
    }

    private static String buildProfileId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    private static String buildMasterDocId(String tenantId, String appId, String masterId) {
        return tenantId + "|" + appId + "|" + masterId;
    }
}