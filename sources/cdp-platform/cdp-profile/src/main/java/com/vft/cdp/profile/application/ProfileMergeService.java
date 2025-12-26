package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.MasterProfile;
import com.vft.cdp.profile.api.response.AutoMergeResponse;
import com.vft.cdp.profile.infra.es.*;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import com.vft.cdp.profile.infra.es.mapper.MasterProfileMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * ENHANCED PROFILE MERGE SERVICE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * GUARANTEES:
 * 1. UNIQUENESS: One person = One master profile (always)
 * 2. IDEMPOTENCY: Running merge multiple times = Same result
 * 3. UPDATE: New matching profile → Update existing master
 * 4. CONFLICT RESOLUTION: Multiple masters for same person → Merge masters
 *
 * MECHANISM:
 * - Before creating master: Check if profiles already have masters
 * - If master exists: Update existing master (don't create new)
 * - If multiple masters: Merge masters into one
 * - Mark original profiles to prevent re-merge
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileMergeService {

    private final SpringDataMasterProfileRepository masterProfileRepo;
    private final SpringDataProfileRepository profileRepo;
    private final DuplicateDetectionService duplicateDetector;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // AUTO MERGE - WITH UNIQUENESS GUARANTEE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * AUTO MERGE - Enhanced version
     *
     * FLOW:
     * 1. Detect duplicate groups
     * 2. For each group:
     *    a. Check if profiles already have masters
     *    b. If yes → Update existing master
     *    c. If no → Create new master
     * 3. Resolve master conflicts (merge overlapping masters)
     */
    @Transactional
    public AutoMergeResult autoMerge(
            String tenantId,
            String mergeStrategy,
            Boolean dryRun,
            Integer maxGroups) {

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🤖 ENHANCED AUTO MERGE");
        log.info("  Tenant: {}", tenantId);
        log.info("  Strategy: {}", mergeStrategy);
        log.info("  Dry Run: {}", dryRun);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        long startTime = System.currentTimeMillis();

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 1: DETECT DUPLICATES
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        Map<String, List<EnrichedProfile>> duplicateGroups =
                duplicateDetector.findDuplicatesByStrategy(tenantId, mergeStrategy);

        if (duplicateGroups.isEmpty()) {
            log.info("✅ No duplicates found");
            return buildEmptyResult();
        }

        // Apply max groups limit
        Map<String, List<EnrichedProfile>> groupsToProcess = duplicateGroups;
        if (maxGroups != null && maxGroups > 0) {
            groupsToProcess = duplicateGroups.entrySet().stream()
                    .limit(maxGroups)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        log.info("📊 Found {} duplicate groups (processing {})",
                duplicateGroups.size(), groupsToProcess.size());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 2: DRY RUN - PREVIEW ONLY
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        if (Boolean.TRUE.equals(dryRun)) {
            return buildDryRunResult(groupsToProcess, startTime);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 3: EXECUTE MERGE WITH UNIQUENESS CHECK
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        int mastersCreated = 0;
        int mastersUpdated = 0;
        int profilesMerged = 0;
        List<AutoMergeResponse.MergeDetail> details = new ArrayList<>();

        for (Map.Entry<String, List<EnrichedProfile>> entry : groupsToProcess.entrySet()) {
            String groupKey = entry.getKey();
            List<EnrichedProfile> profiles = entry.getValue();

            try {
                log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                log.info("🔀 Processing group: {}", groupKey);
                log.info("  Profiles: {}", profiles.size());

                // ✅ CRITICAL: Check existing masters FIRST
                MergeDecision decision = decideMergeAction(profiles);

                String masterProfileId;
                boolean created = false;

                switch (decision.getAction()) {
                    case CREATE_NEW:
                        // No existing master → Create new
                        masterProfileId = createNewMaster(profiles, groupKey);
                        mastersCreated++;
                        created = true;
                        log.info("  ✅ Created new master: {}", masterProfileId);
                        break;

                    case UPDATE_EXISTING:
                        // Has existing master → Update it
                        masterProfileId = updateExistingMaster(
                                decision.getExistingMaster(),
                                profiles
                        );
                        mastersUpdated++;
                        log.info("  ✅ Updated existing master: {}", masterProfileId);
                        break;

                    case MERGE_MASTERS:
                        // Multiple masters → Merge them
                        masterProfileId = mergeMultipleMasters(
                                decision.getExistingMasters(),
                                profiles
                        );
                        mastersCreated++;
                        created = true;
                        log.info("  ✅ Merged {} masters into: {}",
                                decision.getExistingMasters().size(), masterProfileId);
                        break;

                    default:
                        log.warn("  ⚠️  Unknown action: {}", decision.getAction());
                        continue;
                }

                profilesMerged += profiles.size();

                // Build detail
                details.add(AutoMergeResponse.MergeDetail.builder()
                        .masterProfileId(masterProfileId)
                        .matchStrategy(extractStrategy(groupKey))
                        .profilesMerged(profiles.stream()
                                .map(p -> ProfileMapper.buildId(
                                        p.getTenantId(), p.getAppId(), p.getUserId()))
                                .collect(Collectors.toList()))
                        .confidence(determineConfidence(groupKey))
                        .build());

            } catch (Exception ex) {
                log.error("❌ Failed to merge group: {}", groupKey, ex);
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 4: FINAL CONFLICT RESOLUTION
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        int mastersMerged = resolveRemainingConflicts(tenantId);

        long duration = System.currentTimeMillis() - startTime;

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("✅ ENHANCED AUTO MERGE COMPLETED");
        log.info("  Masters created: {}", mastersCreated);
        log.info("  Masters updated: {}", mastersUpdated);
        log.info("  Masters merged: {}", mastersMerged);
        log.info("  Profiles processed: {}", profilesMerged);
        log.info("  Duration: {}ms", duration);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        return AutoMergeResult.builder()
                .duplicateGroupsFound(duplicateGroups.size())
                .masterProfilesCreated(mastersCreated + mastersMerged)
                .totalProfilesMerged(profilesMerged)
                .mergeDetails(details)
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CORE LOGIC: DECIDE MERGE ACTION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Decide what to do with this group of profiles
     *
     * LOGIC:
     * 1. Check if any profile already has master
     * 2. If none → CREATE_NEW
     * 3. If one → UPDATE_EXISTING
     * 4. If multiple → MERGE_MASTERS
     */
    private MergeDecision decideMergeAction(List<EnrichedProfile> profiles) {

        // Get profile IDs
        List<String> profileIds = profiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        // Find existing masters for these profiles
        Set<MasterProfileDocument> existingMasters = new HashSet<>();

        for (String profileId : profileIds) {
            List<MasterProfileDocument> masters =
                    masterProfileRepo.findByMergedIdsContaining(profileId);
            existingMasters.addAll(masters);
        }

        log.debug("  🔍 Found {} existing masters for {} profiles",
                existingMasters.size(), profiles.size());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // DECISION TREE
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (existingMasters.isEmpty()) {
            // Case 1: No existing master → Create new
            return MergeDecision.builder()
                    .action(MergeAction.CREATE_NEW)
                    .build();
        }

        if (existingMasters.size() == 1) {
            // Case 2: One existing master → Update it
            return MergeDecision.builder()
                    .action(MergeAction.UPDATE_EXISTING)
                    .existingMaster(existingMasters.iterator().next())
                    .build();
        }

        // Case 3: Multiple existing masters → Merge them
        return MergeDecision.builder()
                .action(MergeAction.MERGE_MASTERS)
                .existingMasters(new ArrayList<>(existingMasters))
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ACTION 1: CREATE NEW MASTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Create new master profile
     */
    private String createNewMaster(List<EnrichedProfile> profiles, String source) {

        // 1. Build master profile
        MasterProfile masterProfile = MasterProfileMapper.mergeProfiles(profiles);

        // 2. Save to ES
        MasterProfileDocument doc = MasterProfileMapper.toDocument(masterProfile);
        MasterProfileDocument saved = masterProfileRepo.save(doc);

        log.info("  💾 New master saved: {}", saved.getProfileId());

        // 3. Mark original profiles as merged
        markProfilesAsMerged(profiles, saved.getProfileId());

        return saved.getProfileId();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ACTION 2: UPDATE EXISTING MASTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Update existing master with new profiles
     *
     * LOGIC:
     * 1. Load current master
     * 2. Add new profile IDs to merged_ids
     * 3. Merge traits (keep best values)
     * 4. Update timestamps
     * 5. Increment version
     * 6. Save back to ES
     */
    private String updateExistingMaster(
            MasterProfileDocument existingMaster,
            List<EnrichedProfile> newProfiles) {

        log.info("  🔄 Updating master: {}", existingMaster.getProfileId());

        // 1. Get new profile IDs
        List<String> newProfileIds = newProfiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        // 2. Filter out already merged profiles
        List<String> currentMergedIds = existingMaster.getMergedIds() != null
                ? existingMaster.getMergedIds()
                : new ArrayList<>();

        List<String> toAdd = newProfileIds.stream()
                .filter(id -> !currentMergedIds.contains(id))
                .collect(Collectors.toList());

        if (toAdd.isEmpty()) {
            log.info("  ⚠️  All profiles already merged into this master");
            return existingMaster.getProfileId();
        }

        log.info("  ➕ Adding {} new profiles to master", toAdd.size());

        // 3. Update merged_ids
        List<String> updatedMergedIds = new ArrayList<>(currentMergedIds);
        updatedMergedIds.addAll(toAdd);
        existingMaster.setMergedIds(updatedMergedIds);

        // 4. Merge traits from new profiles
        updateMasterTraits(existingMaster, newProfiles);

        // 5. Update metadata
        if (existingMaster.getMetadata() != null) {
            existingMaster.getMetadata().setUpdatedAt(Instant.now());
            existingMaster.getMetadata().setLastSeenAt(Instant.now());

            Integer currentVersion = existingMaster.getMetadata().getVersion();
            existingMaster.getMetadata().setVersion(
                    currentVersion != null ? currentVersion + 1 : 1
            );
        }

        // 6. Save
        MasterProfileDocument saved = masterProfileRepo.save(existingMaster);

        log.info("  ✅ Master updated: {} profiles now",
                saved.getMergedIds().size());

        // 7. Mark new profiles as merged
        markProfilesAsMerged(newProfiles, saved.getProfileId());

        return saved.getProfileId();
    }

    /**
     * Update master traits with new profile data
     *
     * STRATEGY:
     * - Lists (email, phone, userId): Add unique values
     * - Strings (name, gender, dob): Keep latest non-null
     */
    private void updateMasterTraits(
            MasterProfileDocument master,
            List<EnrichedProfile> newProfiles) {

        if (master.getTraits() == null) {
            master.setTraits(new MasterProfileDocument.TraitsDocument());
        }

        MasterProfileDocument.TraitsDocument traits = master.getTraits();

        // Update list fields (add unique values)
        for (EnrichedProfile profile : newProfiles) {
            if (profile.getTraits() == null) continue;

            // Email
            if (profile.getTraits().getEmail() != null) {
                if (traits.getEmail() == null) {
                    traits.setEmail(new ArrayList<>());
                }
                if (!traits.getEmail().contains(profile.getTraits().getEmail())) {
                    traits.getEmail().add(profile.getTraits().getEmail());
                }
            }

            // Phone
            if (profile.getTraits().getPhone() != null) {
                if (traits.getPhone() == null) {
                    traits.setPhone(new ArrayList<>());
                }
                if (!traits.getPhone().contains(profile.getTraits().getPhone())) {
                    traits.getPhone().add(profile.getTraits().getPhone());
                }
            }

            // User ID
            if (profile.getUserId() != null) {
                if (traits.getUserId() == null) {
                    traits.setUserId(new ArrayList<>());
                }
                if (!traits.getUserId().contains(profile.getUserId())) {
                    traits.getUserId().add(profile.getUserId());
                }
            }

            // Update scalar fields (keep latest non-null)
            if (profile.getTraits().getFirstName() != null) {
                traits.setFirstName(profile.getTraits().getFirstName());
            }
            if (profile.getTraits().getLastName() != null) {
                traits.setLastName(profile.getTraits().getLastName());
            }
            if (profile.getTraits().getGender() != null) {
                traits.setGender(profile.getTraits().getGender());
            }
            if (profile.getTraits().getDob() != null) {
                traits.setDob(profile.getTraits().getDob());
            }
            if (profile.getTraits().getAddress() != null) {
                traits.setAddress(profile.getTraits().getAddress());
            }
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ACTION 3: MERGE MULTIPLE MASTERS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Merge multiple existing masters into one
     *
     * SCENARIO:
     * Master1: {ProfileA, ProfileB} (matched by idcard)
     * Master2: {ProfileB, ProfileC} (matched by phone_dob)
     *
     * → Merge into Master1, delete Master2
     */
    private String mergeMultipleMasters(
            List<MasterProfileDocument> existingMasters,
            List<EnrichedProfile> newProfiles) {

        log.info("  🔀 Merging {} existing masters", existingMasters.size());

        // 1. Choose primary master (earliest created)
        MasterProfileDocument primaryMaster = existingMasters.stream()
                .min(Comparator.comparing(m ->
                        m.getMetadata() != null ? m.getMetadata().getCreatedAt() : Instant.now()))
                .orElse(existingMasters.get(0));

        log.info("  👑 Primary master: {}", primaryMaster.getProfileId());

        // 2. Collect all profile IDs from all masters
        Set<String> allProfileIds = new HashSet<>();

        for (MasterProfileDocument master : existingMasters) {
            if (master.getMergedIds() != null) {
                allProfileIds.addAll(master.getMergedIds());
            }
        }

        // Add new profiles
        newProfiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .forEach(allProfileIds::add);

        // 3. Update primary master with all profiles
        primaryMaster.setMergedIds(new ArrayList<>(allProfileIds));

        // 4. Merge traits from all masters
        for (MasterProfileDocument master : existingMasters) {
            if (master.getProfileId().equals(primaryMaster.getProfileId())) {
                continue;  // Skip primary
            }
            mergeTraitsFromMaster(primaryMaster, master);
        }

        // 5. Update metadata
        if (primaryMaster.getMetadata() != null) {
            primaryMaster.getMetadata().setUpdatedAt(Instant.now());

            Integer currentVersion = primaryMaster.getMetadata().getVersion();
            primaryMaster.getMetadata().setVersion(
                    currentVersion != null ? currentVersion + 1 : 1
            );
        }

        // 6. Save primary master
        MasterProfileDocument saved = masterProfileRepo.save(primaryMaster);

        log.info("  ✅ Primary master saved with {} profiles",
                saved.getMergedIds().size());

        // 7. Delete other masters
        for (MasterProfileDocument master : existingMasters) {
            if (!master.getProfileId().equals(primaryMaster.getProfileId())) {
                masterProfileRepo.deleteById(master.getProfileId());
                log.info("  🗑️  Deleted master: {}", master.getProfileId());
            }
        }

        // 8. Update all profile references
        markProfilesAsMerged(newProfiles, saved.getProfileId());

        // Update profiles from deleted masters
        for (MasterProfileDocument deletedMaster : existingMasters) {
            if (!deletedMaster.getProfileId().equals(primaryMaster.getProfileId())) {
                updateProfileReferences(deletedMaster.getMergedIds(), saved.getProfileId());
            }
        }

        return saved.getProfileId();
    }

    /**
     * Merge traits from source master to target master
     */
    private void mergeTraitsFromMaster(
            MasterProfileDocument target,
            MasterProfileDocument source) {

        if (source.getTraits() == null) return;
        if (target.getTraits() == null) {
            target.setTraits(new MasterProfileDocument.TraitsDocument());
        }

        MasterProfileDocument.TraitsDocument targetTraits = target.getTraits();
        MasterProfileDocument.TraitsDocument sourceTraits = source.getTraits();

        // Merge list fields
        if (sourceTraits.getEmail() != null) {
            if (targetTraits.getEmail() == null) {
                targetTraits.setEmail(new ArrayList<>());
            }
            sourceTraits.getEmail().stream()
                    .filter(email -> !targetTraits.getEmail().contains(email))
                    .forEach(targetTraits.getEmail()::add);
        }

        if (sourceTraits.getPhone() != null) {
            if (targetTraits.getPhone() == null) {
                targetTraits.setPhone(new ArrayList<>());
            }
            sourceTraits.getPhone().stream()
                    .filter(phone -> !targetTraits.getPhone().contains(phone))
                    .forEach(targetTraits.getPhone()::add);
        }

        if (sourceTraits.getUserId() != null) {
            if (targetTraits.getUserId() == null) {
                targetTraits.setUserId(new ArrayList<>());
            }
            sourceTraits.getUserId().stream()
                    .filter(userId -> !targetTraits.getUserId().contains(userId))
                    .forEach(targetTraits.getUserId()::add);
        }

        // Merge scalar fields (keep non-null)
        if (targetTraits.getFirstName() == null && sourceTraits.getFirstName() != null) {
            targetTraits.setFirstName(sourceTraits.getFirstName());
        }
        if (targetTraits.getLastName() == null && sourceTraits.getLastName() != null) {
            targetTraits.setLastName(sourceTraits.getLastName());
        }
        if (targetTraits.getGender() == null && sourceTraits.getGender() != null) {
            targetTraits.setGender(sourceTraits.getGender());
        }
        if (targetTraits.getDob() == null && sourceTraits.getDob() != null) {
            targetTraits.setDob(sourceTraits.getDob());
        }
        if (targetTraits.getAddress() == null && sourceTraits.getAddress() != null) {
            targetTraits.setAddress(sourceTraits.getAddress());
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER: MARK PROFILES AS MERGED
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Mark profiles as merged (update metadata.merged_to)
     */
    private void markProfilesAsMerged(
            List<EnrichedProfile> profiles,
            String masterProfileId) {

        for (EnrichedProfile profile : profiles) {
            try {
                String profileId = ProfileMapper.buildId(
                        profile.getTenantId(),
                        profile.getAppId(),
                        profile.getUserId()
                );

                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);
                if (doc != null) {
                    if (doc.getMetadata() == null) {
                        doc.setMetadata(new HashMap<>());
                    }

                    doc.getMetadata().put("merged_to", masterProfileId);
                    doc.getMetadata().put("merged_at", Instant.now().toString());
                    doc.getMetadata().put("status", "merged");

                    profileRepo.save(doc);
                    log.debug("    ✅ Marked as merged: {}", profileId);
                }
            } catch (Exception ex) {
                log.error("    ❌ Failed to mark profile as merged: {}",
                        profile.getUserId(), ex);
            }
        }
    }

    /**
     * Update profile references after master merge
     */
    private void updateProfileReferences(
            List<String> profileIds,
            String newMasterProfileId) {

        if (profileIds == null) return;

        for (String profileId : profileIds) {
            try {
                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);
                if (doc != null) {
                    if (doc.getMetadata() == null) {
                        doc.setMetadata(new HashMap<>());
                    }

                    doc.getMetadata().put("merged_to", newMasterProfileId);
                    doc.getMetadata().put("merged_at", Instant.now().toString());

                    profileRepo.save(doc);
                }
            } catch (Exception ex) {
                log.error("Failed to update profile reference: {}", profileId, ex);
            }
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STEP 4: RESOLVE REMAINING CONFLICTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find and merge remaining conflicting masters
     *
     * Example:
     * Master1: email=[a@gmail.com], phone=[0987]
     * Master2: email=[a@gmail.com], phone=[0912]
     *
     * → Both have same email → Merge them!
     */
    private int resolveRemainingConflicts(String tenantId) {
        log.info("🔍 Resolving remaining conflicts...");

        List<MasterProfileDocument> allMasters = masterProfileRepo.findByTenantId(tenantId);

        if (allMasters.size() < 2) {
            log.info("  ✅ No conflicts (only {} masters)", allMasters.size());
            return 0;
        }

        // Group masters by matching fields
        Map<String, List<MasterProfileDocument>> groups = new HashMap<>();

        for (MasterProfileDocument master : allMasters) {
            if (master.getTraits() == null) continue;

            // Group by email
            if (master.getTraits().getEmail() != null) {
                for (String email : master.getTraits().getEmail()) {
                    String key = "email:" + email;
                    groups.computeIfAbsent(key, k -> new ArrayList<>()).add(master);
                }
            }

            // Group by phone
            if (master.getTraits().getPhone() != null) {
                for (String phone : master.getTraits().getPhone()) {
                    String key = "phone:" + phone;
                    groups.computeIfAbsent(key, k -> new ArrayList<>()).add(master);
                }
            }
        }

        // Find groups with 2+ masters
        int mergedCount = 0;
        Set<String> processed = new HashSet<>();

        for (Map.Entry<String, List<MasterProfileDocument>> entry : groups.entrySet()) {
            List<MasterProfileDocument> conflictingMasters = entry.getValue();

            if (conflictingMasters.size() < 2) continue;

            // Get unique master IDs
            List<String> masterIds = conflictingMasters.stream()
                    .map(MasterProfileDocument::getProfileId)
                    .distinct()
                    .collect(Collectors.toList());

            if (masterIds.size() < 2) continue;

            // Create unique key for this set
            String setKey = masterIds.stream().sorted().collect(Collectors.joining("|"));

            if (processed.contains(setKey)) continue;
            processed.add(setKey);

            log.info("  🔀 Merging {} conflicting masters: {}",
                    masterIds.size(), entry.getKey());

            // Merge them
            try {
                mergeMultipleMasters(conflictingMasters, new ArrayList<>());
                mergedCount++;
            } catch (Exception ex) {
                log.error("  ❌ Failed to merge conflicting masters", ex);
            }
        }

        log.info("  ✅ Resolved {} conflicts", mergedCount);
        return mergedCount;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // MANUAL MERGE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Transactional
    public MasterProfile manualMerge(
            String tenantId,
            List<String> profileIds,
            Boolean forceMerge,
            Boolean keepOriginals) {

        log.info("👤 MANUAL MERGE: {} profiles", profileIds.size());

        // Load profiles
        List<EnrichedProfile> profiles = loadProfiles(profileIds);

        if (profiles.isEmpty()) {
            throw new IllegalArgumentException("No profiles found");
        }

        if (profiles.size() < 2) {
            throw new IllegalArgumentException("At least 2 profiles required");
        }

        // Validate same tenant
        boolean sameTenant = profiles.stream()
                .allMatch(p -> tenantId.equals(p.getTenantId()));

        if (!sameTenant) {
            throw new IllegalArgumentException("All profiles must belong to same tenant");
        }

        // Decide action
        MergeDecision decision = decideMergeAction(profiles);

        String masterProfileId;

        switch (decision.getAction()) {
            case CREATE_NEW:
                masterProfileId = createNewMaster(profiles, "manual");
                break;

            case UPDATE_EXISTING:
                masterProfileId = updateExistingMaster(decision.getExistingMaster(), profiles);
                break;

            case MERGE_MASTERS:
                masterProfileId = mergeMultipleMasters(decision.getExistingMasters(), profiles);
                break;

            default:
                throw new IllegalStateException("Unknown action: " + decision.getAction());
        }

        // Load and return master
        MasterProfileDocument doc = masterProfileRepo.findById(masterProfileId)
                .orElseThrow(() -> new IllegalArgumentException("Master not found"));

        return MasterProfileMapper.toDomain(doc);
    }

    /**
     * GET MASTER PROFILE
     */
    public MasterProfile getMasterProfile(String masterProfileId) {
        MasterProfileDocument doc = masterProfileRepo.findById(masterProfileId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Master profile not found: " + masterProfileId));

        return MasterProfileMapper.toDomain(doc);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER METHODS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private List<EnrichedProfile> loadProfiles(List<String> profileIds) {
        return profileIds.stream()
                .map(id -> profileRepo.findById(id).orElse(null))
                .filter(Objects::nonNull)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());
    }

    private AutoMergeResult buildEmptyResult() {
        return AutoMergeResult.builder()
                .duplicateGroupsFound(0)
                .masterProfilesCreated(0)
                .totalProfilesMerged(0)
                .mergeDetails(new ArrayList<>())
                .build();
    }

    private AutoMergeResult buildDryRunResult(
            Map<String, List<EnrichedProfile>> groups,
            long startTime) {

        int totalProfiles = groups.values().stream()
                .mapToInt(List::size)
                .sum();

        List<AutoMergeResponse.MergeDetail> details = groups.entrySet().stream()
                .map(entry -> {
                    // Check existing masters for preview
                    List<EnrichedProfile> profiles = entry.getValue();
                    MergeDecision decision = decideMergeAction(profiles);

                    return AutoMergeResponse.MergeDetail.builder()
                            .matchStrategy(extractStrategy(entry.getKey()))
                            .profilesMerged(profiles.stream()
                                    .map(p -> ProfileMapper.buildId(
                                            p.getTenantId(), p.getAppId(), p.getUserId()))
                                    .collect(Collectors.toList()))
                            .confidence(determineConfidence(entry.getKey()))
                            .build();
                })
                .collect(Collectors.toList());

        return AutoMergeResult.builder()
                .duplicateGroupsFound(groups.size())
                .masterProfilesCreated(0)  // Dry run
                .totalProfilesMerged(totalProfiles)
                .mergeDetails(details)
                .build();
    }

    private String extractStrategy(String groupKey) {
        if (groupKey.startsWith("idcard:")) return "idcard";
        if (groupKey.startsWith("phone_dob:")) return "phone_dob";
        if (groupKey.startsWith("email_name:")) return "email_name";
        if (groupKey.startsWith("phone_name:")) return "phone_name";
        return "unknown";
    }

    private String determineConfidence(String groupKey) {
        if (groupKey.startsWith("idcard:")) return "high";
        if (groupKey.startsWith("phone_dob:")) return "high";
        if (groupKey.startsWith("email_name:")) return "medium";
        return "low";
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DATA CLASSES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Data
    @Builder
    public static class MergeDecision {
        private MergeAction action;
        private MasterProfileDocument existingMaster;
        private List<MasterProfileDocument> existingMasters;
    }

    public enum MergeAction {
        CREATE_NEW,          // No existing master
        UPDATE_EXISTING,     // One existing master
        MERGE_MASTERS        // Multiple existing masters
    }

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