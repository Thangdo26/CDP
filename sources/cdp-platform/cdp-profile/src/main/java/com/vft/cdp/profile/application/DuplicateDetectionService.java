package com.vft.cdp.profile.application;

import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * DUPLICATE DETECTION SERVICE - FIXED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ FIX: Changed from EnrichedProfile to Domain Profile
 * ✅ FIX: Only loads ACTIVE profiles
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DuplicateDetectionService {

    private final com.vft.cdp.profile.application.repository.ProfileRepository profileRepository;

    private static final int PAGE_SIZE = 100;
    private static final int MAX_PAGES = 10;

    /**
     * Find duplicate profiles by strategy
     * ✅ Returns Domain Profile (not EnrichedProfile)
     */
    public Map<String, List<Profile>> findDuplicatesByStrategy(
            String tenantId,
            String strategy) {

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🔍 DUPLICATE DETECTION (ACTIVE PROFILES ONLY)");
        log.info("  Tenant: {}", tenantId);
        log.info("  Strategy: {}", strategy);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Load ACTIVE profiles only
        List<Profile> allProfiles = loadActiveProfiles(tenantId);

        log.info("📊 Loaded {} ACTIVE profiles", allProfiles.size());

        if (allProfiles.size() < 2) {
            log.warn("⚠️  Not enough profiles to detect duplicates (minimum: 2)");
            return Collections.emptyMap();
        }

        // Apply strategy
        Map<String, List<Profile>> duplicateGroups;

        if ("all".equalsIgnoreCase(strategy)) {
            duplicateGroups = new HashMap<>();
            duplicateGroups.putAll(findByIdCard(allProfiles));
            duplicateGroups.putAll(findByPhoneDob(allProfiles));
            duplicateGroups.putAll(findByEmailName(allProfiles));
            duplicateGroups.putAll(findByPhoneName(allProfiles));
        } else {
            duplicateGroups = switch (strategy.toLowerCase()) {
                case "idcard_only", "idcard" -> findByIdCard(allProfiles);
                case "phone_dob" -> findByPhoneDob(allProfiles);
                case "email_name" -> findByEmailName(allProfiles);
                case "phone_name" -> findByPhoneName(allProfiles);
                default -> {
                    log.warn("⚠️  Unknown strategy: {}. Using 'all'", strategy);
                    Map<String, List<Profile>> all = new HashMap<>();
                    all.putAll(findByIdCard(allProfiles));
                    all.putAll(findByPhoneDob(allProfiles));
                    all.putAll(findByEmailName(allProfiles));
                    all.putAll(findByPhoneName(allProfiles));
                    yield all;
                }
            };
        }

        log.info("✅ Found {} duplicate groups", duplicateGroups.size());
        duplicateGroups.forEach((key, profiles) ->
                log.info("  📦 Group {}: {} profiles", key, profiles.size())
        );

        return duplicateGroups;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // LOAD ACTIVE PROFILES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * ✅ FIX: Load ACTIVE profiles using Application layer repository
     */
    private List<Profile> loadActiveProfiles(String tenantId) {
        List<Profile> allProfiles = new ArrayList<>();
        int currentPage = 0;
        boolean hasMore = true;

        while (hasMore && currentPage < MAX_PAGES) {
            log.debug("  📄 Loading page {} (size: {})", currentPage, PAGE_SIZE);

            // Use Application repository's findActiveProfiles
            Page<com.vft.cdp.profile.application.model.ProfileModel> page =
                    profileRepository.findActiveProfiles(
                            tenantId,
                            org.springframework.data.domain.PageRequest.of(currentPage, PAGE_SIZE)
                    );

            // Convert ProfileModel to Domain Profile
            List<Profile> activeProfiles = page.getContent().stream()
                    .map(this::convertToDomain)
                    .collect(Collectors.toList());

            allProfiles.addAll(activeProfiles);

            log.debug("  ✅ Loaded {} active profiles", activeProfiles.size());

            hasMore = page.hasNext();
            currentPage++;
        }

        if (hasMore) {
            log.warn("⚠️  Reached max pages limit ({}). Total loaded: {}",
                    MAX_PAGES, allProfiles.size());
        }

        log.info("📊 Total ACTIVE profiles loaded: {} from {} pages",
                allProfiles.size(), currentPage);

        return allProfiles;
    }

    /**
     * Convert ProfileModel to Domain Profile
     */
    private Profile convertToDomain(com.vft.cdp.profile.application.model.ProfileModel model) {
        if (model instanceof Profile) {
            return (Profile) model;
        }

        // Reconstruct
        return Profile.builder()
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())
                .status(ProfileStatus.fromValue(model.getStatus()))
                .mergedToMasterId(model.getMergedToMasterId())
                .mergedAt(model.getMergedAt())
                .traits(convertTraits(model.getTraits()))
                .platforms(convertPlatforms(model.getPlatforms()))
                .campaign(convertCampaign(model.getCampaign()))
                .metadata(model.getMetadata())
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .version(model.getVersion())
                .build();
    }

    private Profile.Traits convertTraits(com.vft.cdp.profile.application.model.ProfileModel.TraitsModel traits) {
        if (traits == null) return null;
        if (traits instanceof Profile.Traits) return (Profile.Traits) traits;

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

    private Profile.Platforms convertPlatforms(com.vft.cdp.profile.application.model.ProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;
        if (platforms instanceof Profile.Platforms) return (Profile.Platforms) platforms;

        return Profile.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private Profile.Campaign convertCampaign(com.vft.cdp.profile.application.model.ProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;
        if (campaign instanceof Profile.Campaign) return (Profile.Campaign) campaign;

        return Profile.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MATCHING STRATEGIES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Map<String, List<Profile>> findByIdCard(List<Profile> profiles) {
        log.debug("🔍 Applying strategy: idcard_only");

        Map<String, List<Profile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getIdcard() != null
                        && !p.getTraits().getIdcard().isBlank())
                .collect(Collectors.groupingBy(p -> p.getTraits().getIdcard()));

        Map<String, List<Profile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .collect(Collectors.toMap(
                        e -> "idcard:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by idcard", duplicates.size());
        return duplicates;
    }

    private Map<String, List<Profile>> findByPhoneDob(List<Profile> profiles) {
        log.debug("🔍 Applying strategy: phone_dob");

        Map<String, List<Profile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getPhone() != null
                        && p.getTraits().getDob() != null)
                .collect(Collectors.groupingBy(p -> {
                    String phone = normalizePhone(p.getTraits().getPhone());
                    String dob = normalizeDob(p.getTraits().getDob());
                    return phone + "|" + dob;
                }));

        Map<String, List<Profile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .filter(e -> !e.getKey().contains("null"))
                .collect(Collectors.toMap(
                        e -> "phone_dob:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by phone_dob", duplicates.size());
        return duplicates;
    }

    private Map<String, List<Profile>> findByEmailName(List<Profile> profiles) {
        log.debug("🔍 Applying strategy: email_name");

        Map<String, List<Profile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getEmail() != null
                        && p.getTraits().getFullName() != null)
                .collect(Collectors.groupingBy(p -> {
                    String email = normalizeEmail(p.getTraits().getEmail());
                    String name = normalizeName(p.getTraits().getFullName());
                    return email + "|" + name;
                }));

        Map<String, List<Profile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .filter(e -> !e.getKey().contains("null"))
                .collect(Collectors.toMap(
                        e -> "email_name:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by email_name", duplicates.size());
        return duplicates;
    }

    private Map<String, List<Profile>> findByPhoneName(List<Profile> profiles) {
        log.debug("🔍 Applying strategy: phone_name");

        Map<String, List<Profile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getPhone() != null
                        && p.getTraits().getFullName() != null)
                .collect(Collectors.groupingBy(p -> {
                    String phone = normalizePhone(p.getTraits().getPhone());
                    String name = normalizeName(p.getTraits().getFullName());
                    return phone + "|" + name;
                }));

        Map<String, List<Profile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .filter(e -> !e.getKey().contains("null"))
                .collect(Collectors.toMap(
                        e -> "phone_name:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by phone_name", duplicates.size());
        return duplicates;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NORMALIZATION HELPERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String normalizePhone(String phone) {
        if (phone == null || phone.isBlank()) return "";
        String digits = phone.replaceAll("[^0-9]", "");
        if (digits.startsWith("84") && digits.length() >= 10) {
            digits = "0" + digits.substring(2);
        }
        return digits;
    }

    private String normalizeDob(String dob) {
        if (dob == null || dob.isBlank()) return "";
        dob = dob.trim();

        if (dob.matches("\\d{4}-\\d{2}-\\d{2}")) return dob;
        if (dob.matches("\\d{2}/\\d{2}/\\d{4}")) {
            String[] parts = dob.split("/");
            return parts[2] + "-" + parts[1] + "-" + parts[0];
        }
        if (dob.matches("\\d{4}/\\d{2}/\\d{2}")) {
            return dob.replace("/", "-");
        }
        if (dob.matches("\\d{2}-\\d{2}-\\d{4}")) {
            String[] parts = dob.split("-");
            return parts[2] + "-" + parts[1] + "-" + parts[0];
        }
        return dob;
    }

    private String normalizeEmail(String email) {
        if (email == null || email.isBlank()) return "";
        return email.trim().toLowerCase();
    }

    private String normalizeName(String name) {
        if (name == null || name.isBlank()) return "";
        name = removeVietnameseAccents(name);
        name = name.toLowerCase();
        name = name.trim().replaceAll("\\s+", " ");
        return name;
    }

    private String removeVietnameseAccents(String text) {
        if (text == null || text.isBlank()) return text;

        text = text.replaceAll("[àáạảãâầấậẩẫăằắặẳẵ]", "a");
        text = text.replaceAll("[èéẹẻẽêềếệểễ]", "e");
        text = text.replaceAll("[ìíịỉĩ]", "i");
        text = text.replaceAll("[òóọỏõôồốộổỗơờớợởỡ]", "o");
        text = text.replaceAll("[ùúụủũưừứựửữ]", "u");
        text = text.replaceAll("[ỳýỵỷỹ]", "y");
        text = text.replaceAll("đ", "d");

        text = text.replaceAll("[ÀÁẠẢÃÂẦẤẬẨẪĂẰẮẶẲẴ]", "A");
        text = text.replaceAll("[ÈÉẸẺẼÊỀẾỆỂỄ]", "E");
        text = text.replaceAll("[ÌÍỊỈĨ]", "I");
        text = text.replaceAll("[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]", "O");
        text = text.replaceAll("[ÙÚỤỦŨƯỪỨỰỬỮ]", "U");
        text = text.replaceAll("[ỲÝỴỶỸ]", "Y");
        text = text.replaceAll("Đ", "D");

        return text;
    }
}