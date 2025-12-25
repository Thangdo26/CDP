package com.vft.cdp.profile.application.merge;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import com.vft.cdp.profile.infra.es.ProfileDocument;
import com.vft.cdp.profile.infra.es.ProfileMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * DUPLICATE DETECTION SERVICE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Detects duplicate profiles using multiple strategies:
 * 1. idcard_only - Match by ID card (CCCD/CMND)
 * 2. phone_dob - Match by phone + date of birth
 * 3. email_name - Match by email + full name
 * 4. phone_name - Match by phone + full name
 *
 * Returns: Map<GroupKey, List<EnrichedProfile>>
 * Example: {"idcard:035195012345" -> [Profile1, Profile2]}
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DuplicateDetectionService {

    private final ProfileRepository profileRepository;

    /**
     * Find duplicates by strategy
     *
     * @param tenantId Tenant to search in
     * @param strategy Strategy to use: "idcard_only", "phone_dob", "email_name", "phone_name", "all"
     * @return Map of duplicate groups: {groupKey -> [profiles]}
     */
    public Map<String, List<EnrichedProfile>> findDuplicatesByStrategy(
            String tenantId,
            String strategy) {

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🔍 DUPLICATE DETECTION");
        log.info("  Tenant: {}", tenantId);
        log.info("  Strategy: {}", strategy);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Load all profiles for tenant
        List<EnrichedProfile> allProfiles = loadAllProfiles(tenantId);

        log.info("📊 Loaded {} profiles", allProfiles.size());

        if (allProfiles.size() < 2) {
            log.warn("⚠️  Not enough profiles to detect duplicates (minimum: 2)");
            return Collections.emptyMap();
        }

        // Apply strategy
        Map<String, List<EnrichedProfile>> duplicateGroups;

        if ("all".equalsIgnoreCase(strategy)) {
            // Apply all strategies
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
    // LOAD PROFILES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Load all profiles for a tenant
     */
    private List<EnrichedProfile> loadAllProfiles(String tenantId) {
        SearchProfileRequest request = SearchProfileRequest.builder()
                .tenantId(tenantId)
                .page(0)
                .pageSize(1000)  // Max 1000 profiles
                .build();

        Page<EnrichedProfile> page = profileRepository.search(request);

        log.debug("  📄 Loaded {} profiles from page", page.getContent().size());

        return page.getContent();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STRATEGY 1: IDCARD ONLY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find duplicates by ID Card (CCCD/CMND)
     *
     * Confidence: 100%
     *
     * Logic:
     * - Group profiles with same idcard
     * - Return groups with 2+ profiles
     */
    private Map<String, List<EnrichedProfile>> findByIdCard(List<EnrichedProfile> profiles) {
        log.debug("🔍 Applying strategy: idcard_only");

        Map<String, List<EnrichedProfile>> groups = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getIdcard() != null
                        && !p.getTraits().getIdcard().isBlank())
                .collect(Collectors.groupingBy(p -> p.getTraits().getIdcard()));

        // Filter: Keep only groups with 2+ profiles
        Map<String, List<EnrichedProfile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .collect(Collectors.toMap(
                        e -> "idcard:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by idcard", duplicates.size());

        return duplicates;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STRATEGY 2: PHONE + DOB
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find duplicates by Phone + DOB
     *
     * Confidence: 95%
     *
     * Logic:
     * - Normalize phone (remove spaces, dashes)
     * - Normalize DOB to YYYY-MM-DD format
     * - Group by phone + dob combination
     */
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

        // Filter: Keep only groups with 2+ profiles
        Map<String, List<EnrichedProfile>> duplicates = groups.entrySet().stream()
                .filter(e -> e.getValue().size() >= 2)
                .filter(e -> !e.getKey().contains("null"))  // Exclude null values
                .collect(Collectors.toMap(
                        e -> "phone_dob:" + e.getKey(),
                        Map.Entry::getValue
                ));

        log.debug("  ✅ Found {} duplicate groups by phone_dob", duplicates.size());

        return duplicates;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STRATEGY 3: EMAIL + NAME
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find duplicates by Email + Full Name
     *
     * Confidence: 85%
     *
     * Logic:
     * - Normalize email (lowercase)
     * - Normalize name (remove accents, lowercase)
     * - Group by email + name combination
     */
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

        // Filter: Keep only groups with 2+ profiles
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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STRATEGY 4: PHONE + NAME
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find duplicates by Phone + Full Name
     *
     * Confidence: 75%
     *
     * Logic:
     * - Normalize phone
     * - Normalize name (remove accents, lowercase)
     * - Group by phone + name combination
     */
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

        // Filter: Keep only groups with 2+ profiles
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

    /**
     * Normalize phone number
     *
     * Examples:
     * - "0987-654-321" → "0987654321"
     * - "+84 987 654 321" → "0987654321"
     * - "0987 654 321" → "0987654321"
     */
    private String normalizePhone(String phone) {
        if (phone == null || phone.isBlank()) {
            return "";
        }

        // Remove all non-digit characters
        String digits = phone.replaceAll("[^0-9]", "");

        // Handle +84 country code
        if (digits.startsWith("84") && digits.length() >= 10) {
            digits = "0" + digits.substring(2);
        }

        return digits;
    }

    /**
     * Normalize date of birth
     *
     * Examples:
     * - "20/03/1995" → "1995-03-20"
     * - "1995-03-20" → "1995-03-20"
     * - "1995/03/20" → "1995-03-20"
     */
    private String normalizeDob(String dob) {
        if (dob == null || dob.isBlank()) {
            return "";
        }

        dob = dob.trim();

        // Already in YYYY-MM-DD format
        if (dob.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return dob;
        }

        // DD/MM/YYYY format
        if (dob.matches("\\d{2}/\\d{2}/\\d{4}")) {
            String[] parts = dob.split("/");
            return parts[2] + "-" + parts[1] + "-" + parts[0];
        }

        // YYYY/MM/DD format
        if (dob.matches("\\d{4}/\\d{2}/\\d{2}")) {
            return dob.replace("/", "-");
        }

        // DD-MM-YYYY format
        if (dob.matches("\\d{2}-\\d{2}-\\d{4}")) {
            String[] parts = dob.split("-");
            return parts[2] + "-" + parts[1] + "-" + parts[0];
        }

        return dob;
    }

    /**
     * Normalize email
     *
     * Examples:
     * - "User@Gmail.Com" → "user@gmail.com"
     * - "  test@example.com  " → "test@example.com"
     */
    private String normalizeEmail(String email) {
        if (email == null || email.isBlank()) {
            return "";
        }

        return email.trim().toLowerCase();
    }

    /**
     * Normalize full name (remove accents, lowercase, trim)
     *
     * Examples:
     * - "Nguyễn Văn A" → "nguyen van a"
     * - "NGUYEN VAN A" → "nguyen van a"
     * - "  Nguyen  Van  A  " → "nguyen van a"
     */
    private String normalizeName(String name) {
        if (name == null || name.isBlank()) {
            return "";
        }

        // Remove Vietnamese accents
        name = removeVietnameseAccents(name);

        // Lowercase
        name = name.toLowerCase();

        // Remove extra spaces
        name = name.trim().replaceAll("\\s+", " ");

        return name;
    }

    /**
     * Remove Vietnamese accents
     */
    private String removeVietnameseAccents(String text) {
        if (text == null || text.isBlank()) {
            return text;
        }

        // Lowercase vowels
        text = text.replaceAll("[àáạảãâầấậẩẫăằắặẳẵ]", "a");
        text = text.replaceAll("[èéẹẻẽêềếệểễ]", "e");
        text = text.replaceAll("[ìíịỉĩ]", "i");
        text = text.replaceAll("[òóọỏõôồốộổỗơờớợởỡ]", "o");
        text = text.replaceAll("[ùúụủũưừứựửữ]", "u");
        text = text.replaceAll("[ỳýỵỷỹ]", "y");
        text = text.replaceAll("đ", "d");

        // Uppercase vowels
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