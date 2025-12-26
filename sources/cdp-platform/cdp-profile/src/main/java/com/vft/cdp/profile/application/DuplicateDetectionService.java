package com.vft.cdp.profile.application;

import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * DUPLICATE DETECTION SERVICE - WITH STATUS FILTER
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ CRITICAL CHANGE:
 * Only loads ACTIVE profiles for duplicate detection.
 * Excludes MERGED and DELETED profiles.
 *
 * WHY:
 * - MERGED profiles already part of a master → skip
 * - DELETED profiles soft-deleted → skip
 * - Only ACTIVE profiles can be merged
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DuplicateDetectionService {

    private final ProfileRepository profileRepository;

    // ✅ Respect page size limit
    private static final int PAGE_SIZE = 100;
    private static final int MAX_PAGES = 10;   // Max 1000 profiles

    /**
     * Find duplicate profiles by strategy
     *
     * ✅ ONLY LOADS ACTIVE PROFILES
     */
    public Map<String, List<Profile>> findDuplicatesByStrategy(
            String tenantId,
            String strategy) {

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🔍 DUPLICATE DETECTION (ACTIVE PROFILES ONLY)");
        log.info("  Tenant: {}", tenantId);
        log.info("  Strategy: {}", strategy);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // ✅ Load ACTIVE profiles only
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
    // ✅ LOAD ACTIVE PROFILES ONLY (NEW LOGIC)
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Load ACTIVE profiles with batch pagination
     *
     * FILTER:
     * - status = "active"
     * - Excludes merged and deleted profiles
     */
    private List<Profile> loadActiveProfiles(String tenantId) {
        List<Profile> allProfiles = new ArrayList<>();

        int currentPage = 0;
        boolean hasMore = true;

        while (hasMore && currentPage < MAX_PAGES) {

            SearchProfileRequest request = SearchProfileRequest.builder()
                    .tenantId(tenantId)
                    .page(currentPage)
                    .pageSize(PAGE_SIZE)
                    .build();

            log.debug("  📄 Loading page {} (size: {})", currentPage, PAGE_SIZE);

            Page<Profile> page = profileRepository.search(request);

            // ✅ CRITICAL: Filter ACTIVE profiles only
            List<Profile> activeProfiles = page.getContent().stream()
                    .filter(p -> p.getStatus() == ProfileStatus.ACTIVE)
                    .collect(Collectors.toList());

            allProfiles.addAll(activeProfiles);

            log.debug("  ✅ Loaded {} profiles ({} active, {} total on page)",
                    activeProfiles.size(),
                    activeProfiles.size(),
                    page.getContent().size());

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