package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * DUPLICATE DETECTION SERVICE - FIXED VERSION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * FIX: Load profiles in batches (PAGE_SIZE=100) to respect limit
 * OLD: pageSize=1000 → Caused "Page size cannot exceed 100" error
 * NEW: Load in batches of 100, up to MAX_PAGES=10 (1000 total)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DuplicateDetectionService {

    private final ProfileRepository profileRepository;

    // ✅ FIX: Respect EsProfileRepository.MAX_PAGE_SIZE = 100
    private static final int PAGE_SIZE = 100;
    private static final int MAX_PAGES = 10;   // Max 1000 profiles (100 * 10)

    public Map<String, List<EnrichedProfile>> findDuplicatesByStrategy(
            String tenantId,
            String strategy) {

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🔍 DUPLICATE DETECTION");
        log.info("  Tenant: {}", tenantId);
        log.info("  Strategy: {}", strategy);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // ✅ FIX: Load with pagination
        List<EnrichedProfile> allProfiles = loadAllProfiles(tenantId);

        log.info("📊 Loaded {} profiles", allProfiles.size());

        if (allProfiles.size() < 2) {
            log.warn("⚠️  Not enough profiles to detect duplicates (minimum: 2)");
            return Collections.emptyMap();
        }

        // Apply strategy
        Map<String, List<EnrichedProfile>> duplicateGroups;

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
                    Map<String, List<EnrichedProfile>> all = new HashMap<>();
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
    // ✅ FIX: LOAD PROFILES WITH BATCH PAGINATION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Load all profiles for a tenant with batch pagination
     *
     * BEFORE:
     * - Request pageSize=1000 in one call
     * - Error: "Page size cannot exceed 100"
     *
     * AFTER:
     * - Load in batches of 100
     * - Loop until all profiles loaded or MAX_PAGES reached
     */
    private List<EnrichedProfile> loadAllProfiles(String tenantId) {
        List<EnrichedProfile> allProfiles = new ArrayList<>();

        int currentPage = 0;
        boolean hasMore = true;

        while (hasMore && currentPage < MAX_PAGES) {

            SearchProfileRequest request = SearchProfileRequest.builder()
                    .tenantId(tenantId)
                    .page(currentPage)
                    .pageSize(PAGE_SIZE)  // ✅ FIX: 100 instead of 1000
                    .build();

            log.debug("  📄 Loading page {} (size: {})", currentPage, PAGE_SIZE);

            Page<EnrichedProfile> page = profileRepository.search(request);

            allProfiles.addAll(page.getContent());

            log.debug("  ✅ Loaded {} profiles (total: {})",
                    page.getContent().size(), allProfiles.size());

            hasMore = page.hasNext();
            currentPage++;
        }

        if (hasMore) {
            log.warn("⚠️  Reached max pages limit ({}). Total loaded: {}",
                    MAX_PAGES, allProfiles.size());
        }

        log.info("📊 Total profiles loaded: {} from {} pages",
                allProfiles.size(), currentPage);

        return allProfiles;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MATCHING STRATEGIES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Map<String, List<EnrichedProfile>> findByIdCard(List<EnrichedProfile> profiles) {
        log.debug("🔍 Applying strategy: idcard_only");

        Map<String, List<EnrichedProfile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getIdcard() != null
                        && !p.getTraits().getIdcard().isBlank())
                .collect(Collectors.groupingBy(p -> p.getTraits().getIdcard()));

        Map<String, List<EnrichedProfile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .collect(Collectors.toMap(
                        e -> "idcard:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by idcard", duplicates.size());
        return duplicates;
    }

    private Map<String, List<EnrichedProfile>> findByPhoneDob(List<EnrichedProfile> profiles) {
        log.debug("🔍 Applying strategy: phone_dob");

        Map<String, List<EnrichedProfile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getPhone() != null
                        && p.getTraits().getDob() != null)
                .collect(Collectors.groupingBy(p -> {
                    String phone = normalizePhone(p.getTraits().getPhone());
                    String dob = normalizeDob(p.getTraits().getDob());
                    return phone + "|" + dob;
                }));

        Map<String, List<EnrichedProfile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .filter(e -> !e.getKey().contains("null"))
                .collect(Collectors.toMap(
                        e -> "phone_dob:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by phone_dob", duplicates.size());
        return duplicates;
    }

    private Map<String, List<EnrichedProfile>> findByEmailName(List<EnrichedProfile> profiles) {
        log.debug("🔍 Applying strategy: email_name");

        Map<String, List<EnrichedProfile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getEmail() != null
                        && p.getTraits().getFullName() != null)
                .collect(Collectors.groupingBy(p -> {
                    String email = normalizeEmail(p.getTraits().getEmail());
                    String name = normalizeName(p.getTraits().getFullName());
                    return email + "|" + name;
                }));

        Map<String, List<EnrichedProfile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .filter(e -> !e.getKey().contains("null"))
                .collect(Collectors.toMap(
                        e -> "email_name:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by email_name", duplicates.size());
        return duplicates;
    }

    private Map<String, List<EnrichedProfile>> findByPhoneName(List<EnrichedProfile> profiles) {
        log.debug("🔍 Applying strategy: phone_name");

        Map<String, List<EnrichedProfile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getPhone() != null
                        && p.getTraits().getFullName() != null)
                .collect(Collectors.groupingBy(p -> {
                    String phone = normalizePhone(p.getTraits().getPhone());
                    String name = normalizeName(p.getTraits().getFullName());
                    return phone + "|" + name;
                }));

        Map<String, List<EnrichedProfile>> duplicates = groups.entrySet().stream()
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