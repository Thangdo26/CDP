package com.vft.cdp.profile.application.merge;

import com.vft.cdp.profile.domain.model.EnrichedProfile;
import com.vft.cdp.profile.api.response.AutoMergeResponse;
import com.vft.cdp.profile.domain.model.MasterProfile;
import com.vft.cdp.profile.infra.es.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileMergeService {

    private final SpringDataMasterProfileRepository masterProfileRepo;
    private final SpringDataProfileRepository profileRepo;
    private final DuplicateDetectionService duplicateDetector;

    /**
     * AUTO MERGE with strategy support
     */
    @Transactional
    public AutoMergeResult autoMerge(
            String tenantId,
            String mergeStrategy,
            Boolean dryRun,
            Integer maxGroups) {

        log.info("🤖 AUTO MERGE: tenant={}, strategy={}, dry_run={}",
                tenantId, mergeStrategy, dryRun);

        // 1. Detect duplicates
        Map<String, List<EnrichedProfile>> duplicateGroups =
                duplicateDetector.findDuplicatesByStrategy(tenantId, mergeStrategy);

        if (duplicateGroups.isEmpty()) {
            log.info("No duplicates found");
            return AutoMergeResult.builder()
                    .duplicateGroupsFound(0)
                    .masterProfilesCreated(0)
                    .totalProfilesMerged(0)
                    .mergeDetails(new ArrayList<>())
                    .build();
        }

        // 2. Apply max groups limit
        Map<String, List<EnrichedProfile>> groupsToProcess = duplicateGroups;
        if (maxGroups != null && maxGroups > 0) {
            groupsToProcess = duplicateGroups.entrySet().stream()
                    .limit(maxGroups)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // 3. Dry run: Only return detection results
        if (Boolean.TRUE.equals(dryRun)) {
            return buildDryRunResult(groupsToProcess);
        }

        // 4. Execute merge
        int mergedCount = 0;
        int totalProfiles = 0;
        List<AutoMergeResponse.MergeDetail> details = new ArrayList<>();

        for (Map.Entry<String, List<EnrichedProfile>> entry : groupsToProcess.entrySet()) {
            String groupKey = entry.getKey();
            List<EnrichedProfile> profiles = entry.getValue();

            try {
                MasterProfile masterProfile = mergeProfileGroup(profiles, "auto_" + groupKey);
                mergedCount++;
                totalProfiles += profiles.size();

                // Add detail
                details.add(AutoMergeResponse.MergeDetail.builder()
                        .masterProfileId(masterProfile.getProfileId())
                        .matchStrategy(extractStrategy(groupKey))
                        .profilesMerged(profiles.stream()
                                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                                .collect(Collectors.toList()))
                        .confidence(determineConfidence(groupKey))
                        .build());

            } catch (Exception ex) {
                log.error("Failed to merge group: {}", groupKey, ex);
            }
        }

        log.info("✅ AUTO MERGE completed: {} master profiles created", mergedCount);

        return AutoMergeResult.builder()
                .duplicateGroupsFound(duplicateGroups.size())
                .masterProfilesCreated(mergedCount)
                .totalProfilesMerged(totalProfiles)
                .mergeDetails(details)
                .build();
    }

    /**
     * MANUAL MERGE with validation
     */
    @Transactional
    public MasterProfile manualMerge(
            String tenantId,
            List<String> profileIds,
            Boolean forceMerge,
            Boolean keepOriginals) {

        log.info("👤 MANUAL MERGE: tenant={}, profiles={}, force={}",
                tenantId, profileIds.size(), forceMerge);

        // 1. Load profiles
        List<EnrichedProfile> profiles = loadProfiles(profileIds);

        if (profiles.isEmpty()) {
            throw new IllegalArgumentException("No profiles found to merge");
        }

        if (profiles.size() < 2) {
            throw new IllegalArgumentException("At least 2 profiles required");
        }

        // 2. Validate same tenant
        boolean sameTenant = profiles.stream()
                .allMatch(p -> tenantId.equals(p.getTenantId()));

        if (!sameTenant) {
            throw new IllegalArgumentException("All profiles must belong to same tenant");
        }

        // 3. Validate duplicate criteria (unless force merge)
        if (!Boolean.TRUE.equals(forceMerge)) {
            validateDuplicateCriteria(profiles);
        }

        // 4. Merge
        MasterProfile masterProfile = mergeProfileGroup(
                profiles,
                "manual",
                keepOriginals
        );

        log.info("✅ MANUAL MERGE completed: master_id={}",
                masterProfile.getProfileId());

        return masterProfile;
    }

    /**
     * GET MASTER PROFILE by ID
     */
    public MasterProfile getMasterProfile(String masterProfileId) {
        MasterProfileDocument doc = masterProfileRepo.findById(masterProfileId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Master profile not found: " + masterProfileId
                ));

        return MasterProfileMapper.toDomain(doc);
    }

    // ========== PRIVATE HELPER METHODS ==========

    /**
     * Core merge logic with optional keep_originals
     */
    private MasterProfile mergeProfileGroup(
            List<EnrichedProfile> profiles,
            String source) {
        return mergeProfileGroup(profiles, source, false);
    }

    private MasterProfile mergeProfileGroup(
            List<EnrichedProfile> profiles,
            String source,
            Boolean keepOriginals) {

        log.info("🔀 Merging {} profiles (source: {}, keep_originals: {})",
                profiles.size(), source, keepOriginals);

        // 1. Check if already merged
        List<String> profileIds = profiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        for (String profileId : profileIds) {
            List<MasterProfileDocument> existing =
                    masterProfileRepo.findByMergedIdsContaining(profileId);
            if (!existing.isEmpty()) {
                log.warn("⚠️  Profile {} already merged into master {}",
                        profileId, existing.get(0).getProfileId());
                // Could handle re-merge here
            }
        }

        // 2. Build master profile
        MasterProfile masterProfile = MasterProfileMapper.mergeProfiles(profiles);

        // 3. Save to master_profiles index
        MasterProfileDocument doc = MasterProfileMapper.toDocument(masterProfile);
        MasterProfileDocument saved = masterProfileRepo.save(doc);

        log.info("💾 Master profile saved: {}", saved.getProfileId());

        // 4. Mark original profiles as merged (unless keep_originals=true)
        if (!Boolean.TRUE.equals(keepOriginals)) {
            markProfilesAsMerged(profiles, saved.getProfileId());
        }

        return masterProfile;
    }

    /**
     * Load profiles by IDs
     */
    private List<EnrichedProfile> loadProfiles(List<String> profileIds) {
        return profileIds.stream()
                .map(id -> profileRepo.findById(id).orElse(null))
                .filter(Objects::nonNull)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());
    }

    /**
     * Mark profiles as merged
     */
    private void markProfilesAsMerged(List<EnrichedProfile> profiles, String masterProfileId) {
        for (EnrichedProfile profile : profiles) {
            try {
                String profileId = ProfileMapper.buildId(
                        profile.getTenantId(),
                        profile.getAppId(),
                        profile.getUserId()
                );

                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);

                if (doc != null) {
                    // Add merge metadata
                    if (doc.getMetadata() == null) {
                        doc.setMetadata(new HashMap<>());
                    }
                    doc.getMetadata().put("merged_to", masterProfileId);
                    doc.getMetadata().put("merged_at", java.time.Instant.now().toString());
                    doc.getMetadata().put("status", "merged");

                    profileRepo.save(doc);

                    log.debug("✅ Marked profile as merged: {}", profileId);
                }

            } catch (Exception ex) {
                log.error("❌ Failed to mark profile as merged: {}",
                        profile.getUserId(), ex);
            }
        }
    }

    /**
     * Validate duplicate criteria for manual merge
     */
    private void validateDuplicateCriteria(List<EnrichedProfile> profiles) {
        // Check if profiles match any duplicate criteria

        // Strategy 1: Same idcard
        Set<String> idcards = profiles.stream()
                .map(p -> p.getTraits() != null ? p.getTraits().getIdcard() : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        if (idcards.size() == 1) {
            log.info("✅ Validation passed: Same idcard");
            return;
        }

        // Strategy 2: Same phone + dob
        Set<String> phoneDobs = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getPhone() != null
                        && p.getTraits().getDob() != null)
                .map(p -> p.getTraits().getPhone() + "_" + p.getTraits().getDob())
                .collect(Collectors.toSet());

        if (phoneDobs.size() == 1 && !phoneDobs.contains(null)) {
            log.info("✅ Validation passed: Same phone + dob");
            return;
        }

        // Strategy 3: Same email + full_name
        Set<String> emailNames = profiles.stream()
                .filter(p -> p.getTraits() != null
                        && p.getTraits().getEmail() != null
                        && p.getTraits().getFullName() != null)
                .map(p -> p.getTraits().getEmail() + "_" + p.getTraits().getFullName())
                .collect(Collectors.toSet());

        if (emailNames.size() == 1 && !emailNames.contains(null)) {
            log.info("✅ Validation passed: Same email + full_name");
            return;
        }

        throw new IllegalArgumentException(
                "Profiles do not match duplicate criteria. Use force_merge=true to override."
        );
    }

    /**
     * Build dry run result
     */
    private AutoMergeResult buildDryRunResult(Map<String, List<EnrichedProfile>> groups) {
        int totalProfiles = groups.values().stream()
                .mapToInt(List::size)
                .sum();

        List<AutoMergeResponse.MergeDetail> details = groups.entrySet().stream()
                .map(entry -> AutoMergeResponse.MergeDetail.builder()
                        .matchStrategy(extractStrategy(entry.getKey()))
                        .profilesMerged(entry.getValue().stream()
                                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                                .collect(Collectors.toList()))
                        .confidence(determineConfidence(entry.getKey()))
                        .build())
                .collect(Collectors.toList());

        return AutoMergeResult.builder()
                .duplicateGroupsFound(groups.size())
                .masterProfilesCreated(0)  // Dry run - not created
                .totalProfilesMerged(totalProfiles)
                .mergeDetails(details)
                .build();
    }

    /**
     * Extract strategy from group key
     */
    private String extractStrategy(String groupKey) {
        if (groupKey.startsWith("idcard:")) return "idcard";
        if (groupKey.startsWith("phone_dob:")) return "phone_dob";
        if (groupKey.startsWith("email_name:")) return "email_name";
        return "unknown";
    }

    /**
     * Determine confidence level
     */
    private String determineConfidence(String groupKey) {
        if (groupKey.startsWith("idcard:")) return "high";
        if (groupKey.startsWith("phone_dob:")) return "high";
        if (groupKey.startsWith("email_name:")) return "medium";
        return "low";
    }

    // ========== RESULT OBJECTS ==========

    @Data
    @Builder
    @AllArgsConstructor
    public static class AutoMergeResult {
        private Integer duplicateGroupsFound;
        private Integer masterProfilesCreated;
        private Integer totalProfilesMerged;
        private List<AutoMergeResponse.MergeDetail> mergeDetails;
    }
}