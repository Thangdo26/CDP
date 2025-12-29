package com.vft.cdp.profile.application;

import com.vft.cdp.profile.api.response.AutoMergeResponse;
import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.es.SpringDataMasterProfileRepository;
import com.vft.cdp.profile.infra.es.SpringDataProfileRepository;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import com.vft.cdp.profile.infra.es.mapper.ProfileMapper;
import com.vft.cdp.profile.application.model.ProfileModel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.vft.cdp.profile.infra.es.mapper.MasterProfileMapper;


import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MERGE SERVICE - PURE DOMAIN
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ NO EnrichedProfile - uses Domain Profile only
 * ✅ GUARANTEES:
 * 1. UNIQUENESS: One person = One master profile
 * 2. IDEMPOTENCY: Running merge multiple times = Same result
 * 3. UPDATE: New matching profile → Update existing master
 * 4. CONFLICT RESOLUTION: Multiple masters → Merge masters
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
    // AUTO MERGE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Transactional
    public AutoMergeResult autoMerge(
            String tenantId,
            String mergeStrategy,
            Boolean dryRun,
            Integer maxGroups) {

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🤖 AUTO MERGE (DOMAIN PROFILE)");
        log.info("  Tenant: {}", tenantId);
        log.info("  Strategy: {}", mergeStrategy);
        log.info("  Dry Run: {}", dryRun);
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        long startTime = System.currentTimeMillis();

        // 1. DETECT DUPLICATES
        Map<String, List<Profile>> duplicateGroups =
                duplicateDetector.findDuplicatesByStrategy(tenantId, mergeStrategy);

        if (duplicateGroups.isEmpty()) {
            log.info("✅ No duplicates found");
            return buildEmptyResult();
        }

        // Apply max groups limit
        Map<String, List<Profile>> groupsToProcess = duplicateGroups;
        if (maxGroups != null && maxGroups > 0) {
            groupsToProcess = duplicateGroups.entrySet().stream()
                    .limit(maxGroups)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        log.info("📊 Found {} duplicate groups (processing {})",
                duplicateGroups.size(), groupsToProcess.size());

        // 2. DRY RUN
        if (Boolean.TRUE.equals(dryRun)) {
            return buildDryRunResult(groupsToProcess, startTime);
        }

        // 3. EXECUTE MERGE
        int mastersCreated = 0;
        int mastersUpdated = 0;
        int profilesMerged = 0;
        List<AutoMergeResponse.MergeDetail> details = new ArrayList<>();

        for (Map.Entry<String, List<Profile>> entry : groupsToProcess.entrySet()) {
            String groupKey = entry.getKey();
            List<Profile> profiles = entry.getValue();

            try {
                log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                log.info("🔀 Processing group: {}", groupKey);
                log.info("  Profiles: {}", profiles.size());

                MergeDecision decision = decideMergeAction(profiles);

                String masterProfileId;

                switch (decision.getAction()) {
                    case CREATE_NEW:
                        masterProfileId = createNewMaster(profiles, groupKey);
                        mastersCreated++;
                        log.info("  ✅ Created new master: {}", masterProfileId);
                        break;

                    case UPDATE_EXISTING:
                        masterProfileId = updateExistingMaster(
                                decision.getExistingMaster(),
                                profiles
                        );
                        mastersUpdated++;
                        log.info("  ✅ Updated existing master: {}", masterProfileId);
                        break;

                    case MERGE_MASTERS:
                        masterProfileId = mergeMultipleMasters(
                                decision.getExistingMasters(),
                                profiles
                        );
                        mastersCreated++;
                        log.info("  ✅ Merged {} masters into: {}",
                                decision.getExistingMasters().size(), masterProfileId);
                        break;

                    default:
                        log.warn("  ⚠️  Unknown action: {}", decision.getAction());
                        continue;
                }

                profilesMerged += profiles.size();

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

        // 4. RESOLVE CONFLICTS
        int mastersMerged = resolveRemainingConflicts(tenantId);

        long duration = System.currentTimeMillis() - startTime;

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("✅ AUTO MERGE COMPLETED");
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
    // DECIDE MERGE ACTION
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private MergeDecision decideMergeAction(List<Profile> profiles) {

        List<String> profileIds = profiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 1: Check by merged profile IDs (existing logic)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Set<MasterProfileDocument> existingMasters = new HashSet<>();

        for (String profileId : profileIds) {
            List<MasterProfileDocument> masters =
                    masterProfileRepo.findByMergedProfileIdsContaining(profileId);
            existingMasters.addAll(masters);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 2: Check by IDCARD (NEW - Smart merge)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        if (existingMasters.isEmpty()) {
            // Extract idcards from profiles
            Set<String> idcards = profiles.stream()
                    .map(Profile::getTraits)
                    .filter(Objects::nonNull)
                    .map(traits -> traits.getIdcard())
                    .filter(Objects::nonNull)
                    .filter(idcard -> !idcard.isBlank())
                    .collect(Collectors.toSet());

            // Check if any master profile has matching idcard
            for (String idcard : idcards) {
                List<MasterProfileDocument> mastersByIdcard =
                        masterProfileRepo.findByTraitsIdcard(idcard);

                if (!mastersByIdcard.isEmpty()) {
                    log.info("  🎯 Found {} existing master(s) with same idcard: {}",
                            mastersByIdcard.size(), idcard);
                    existingMasters.addAll(mastersByIdcard);
                }
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 3: Decide action based on findings
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        log.debug("  🔍 Found {} existing masters for {} profiles",
                existingMasters.size(), profiles.size());

        if (existingMasters.isEmpty()) {
            return MergeDecision.builder()
                    .action(MergeAction.CREATE_NEW)
                    .build();
        }

        if (existingMasters.size() == 1) {
            return MergeDecision.builder()
                    .action(MergeAction.UPDATE_EXISTING)
                    .existingMaster(existingMasters.iterator().next())
                    .build();
        }

        // Multiple masters found - need to merge them
        return MergeDecision.builder()
                .action(MergeAction.MERGE_MASTERS)
                .existingMasters(new ArrayList<>(existingMasters))
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ACTION 1: CREATE NEW MASTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String createNewMaster(List<Profile> profiles, String source) {

        MasterProfile masterProfile = MasterProfileMapper.mergeProfiles(profiles);

        MasterProfileDocument doc = MasterProfileMapper.toDocument(masterProfile);
        MasterProfileDocument saved = masterProfileRepo.save(doc);

        log.info("  💾 New master saved: {}", saved.getMasterId());

        markProfilesAsMerged(profiles, saved.getMasterId());

        return saved.getMasterId();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // ACTION 2: UPDATE EXISTING MASTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String updateExistingMaster(
            MasterProfileDocument existingMaster,
            List<Profile> newProfiles) {

        log.info("  🔄 Updating master: {}", existingMaster.getMasterId());

        List<String> newProfileIds = newProfiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        List<String> currentMergedIds = existingMaster.getMergedProfileIds() != null
                ? existingMaster.getMergedProfileIds()
                : new ArrayList<>();

        List<String> toAdd = newProfileIds.stream()
                .filter(id -> !currentMergedIds.contains(id))
                .collect(Collectors.toList());

        if (toAdd.isEmpty()) {
            log.info("  ⚠️  All profiles already merged");
            return existingMaster.getMasterId();
        }

        log.info("  ➕ Adding {} new profiles", toAdd.size());

        // Update merged IDs
        List<String> updatedMergedIds = new ArrayList<>(currentMergedIds);
        updatedMergedIds.addAll(toAdd);
        existingMaster.setMergedProfileIds(updatedMergedIds);
        existingMaster.setMergeCount(updatedMergedIds.size());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ✅ FIX: Combine ALL profiles (existing + new) for comparison
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        // Get ALL merged profiles
        List<Profile> allMergedProfiles = new ArrayList<>(newProfiles);

        // Add existing merged profiles (if we can load them)
        if (currentMergedIds != null && !currentMergedIds.isEmpty()) {
            List<Profile> existingProfiles = loadProfiles(currentMergedIds);
            allMergedProfiles.addAll(existingProfiles);
        }

        // ✅ Find the ABSOLUTE newest profile across ALL profiles
        Profile newestProfile = allMergedProfiles.stream()
                .max(Comparator.comparing(
                        p -> p.getLastSeenAt() != null ? p.getLastSeenAt() : Instant.MIN
                ))
                .orElse(newProfiles.get(0));

        Instant newestLastSeenAt = newestProfile.getLastSeenAt();

        log.info("  🔍 Newest profile: userId={}, last_seen_at={}",
                newestProfile.getUserId(), newestLastSeenAt);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ✅ FIX: Only update if incoming profile is NEWER than master
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        boolean shouldUpdate = false;

        if (existingMaster.getLastSeenAt() == null) {
            shouldUpdate = true;
            log.info("  ✅ Master has no last_seen_at, will update");

        } else if (newestLastSeenAt != null &&
                newestLastSeenAt.isAfter(existingMaster.getLastSeenAt())) {
            shouldUpdate = true;
            log.info("  ✅ Newest profile is NEWER than master: {} > {}",
                    newestLastSeenAt, existingMaster.getLastSeenAt());

        } else {
            log.info("  ⏭️  Newest profile is OLDER than or EQUAL to master, skip trait update");
        }

        // ✅ Update traits/platforms/campaign if newer
        if (shouldUpdate) {
            updateMasterTraitsWithPriority(existingMaster, newestProfile);
            updateMasterPlatformsWithPriority(existingMaster, newestProfile);
            updateMasterCampaignWithPriority(existingMaster, newestProfile);

            // Update last_seen_at
            existingMaster.setLastSeenAt(newestLastSeenAt);
            log.info("  📅 Updated last_seen_at: {}", newestLastSeenAt);
        }

        // Always update metadata
        existingMaster.setUpdatedAt(Instant.now());
        existingMaster.setVersion(
                existingMaster.getVersion() != null ? existingMaster.getVersion() + 1 : 1
        );

        MasterProfileDocument saved = masterProfileRepo.save(existingMaster);

        log.info("  ✅ Master updated: {} profiles, version={}",
                saved.getMergedProfileIds().size(), saved.getVersion());

        markProfilesAsMerged(newProfiles, saved.getMasterId());

        return saved.getMasterId();
    }

    /**
     * ✅ FIXED: Update master traits from NEWEST profile
     *
     * Strategy: ALWAYS update ALL fields from newest profile
     * No more "only if null" logic
     */
    private void updateMasterTraitsWithPriority(
            MasterProfileDocument master,
            Profile newestProfile) {

        if (master.getTraits() == null) {
            master.setTraits(new MasterProfileDocument.Traits());
        }

        // ✅ FIX: Use ProfileModel.TraitsModel (interface) instead of Profile.Traits
        ProfileModel.TraitsModel sourceTraits = newestProfile.getTraits();

        if (sourceTraits == null) {
            return;
        }

        MasterProfileDocument.Traits traits = master.getTraits();

        // ✅ UPDATE ALL FIELDS from newest profile

        if (sourceTraits.getFullName() != null) {
            traits.setFullName(sourceTraits.getFullName());
            log.debug("  📝 Updated fullName: {}", traits.getFullName());
        }

        if (sourceTraits.getFirstName() != null) {
            traits.setFirstName(sourceTraits.getFirstName());
            log.debug("  📝 Updated firstName: {}", traits.getFirstName());
        }

        if (sourceTraits.getLastName() != null) {
            traits.setLastName(sourceTraits.getLastName());
            log.debug("  📝 Updated lastName: {}", traits.getLastName());
        }

        if (sourceTraits.getGender() != null) {
            traits.setGender(sourceTraits.getGender());
            log.debug("  📝 Updated gender: {}", traits.getGender());
        }

        if (sourceTraits.getDob() != null) {
            traits.setDob(sourceTraits.getDob());
            log.debug("  📝 Updated dob: {}", traits.getDob());
        }

        if (sourceTraits.getAddress() != null) {
            traits.setAddress(sourceTraits.getAddress());
            log.debug("  📝 Updated address: {}", traits.getAddress());
        }

        if (sourceTraits.getIdcard() != null) {
            traits.setIdcard(sourceTraits.getIdcard());
            log.debug("  📝 Updated idcard: {}", traits.getIdcard());
        }

        if (sourceTraits.getOldIdcard() != null) {
            traits.setOldIdcard(sourceTraits.getOldIdcard());
            log.debug("  📝 Updated oldIdcard: {}", traits.getOldIdcard());
        }

        if (sourceTraits.getReligion() != null) {
            traits.setReligion(sourceTraits.getReligion());
            log.debug("  📝 Updated religion: {}", traits.getReligion());
        }

        // ✅ Email and Phone - ALWAYS update
        if (sourceTraits.getEmail() != null) {
            traits.setEmail(sourceTraits.getEmail());
            log.debug("  📧 Updated email: {}", traits.getEmail());
        }

        if (sourceTraits.getPhone() != null) {
            traits.setPhone(sourceTraits.getPhone());
            log.debug("  📱 Updated phone: {}", traits.getPhone());
        }
    }

    /**
     * ✅ NEW: Update master platforms from newest profile
     */
    private void updateMasterPlatformsWithPriority(
            MasterProfileDocument master,
            Profile newestProfile) {

        // ✅ FIX: Use ProfileModel.PlatformsModel (interface)
        ProfileModel.PlatformsModel sourcePlatforms = newestProfile.getPlatforms();

        if (sourcePlatforms == null) {
            return;
        }

        if (master.getPlatforms() == null) {
            master.setPlatforms(new MasterProfileDocument.Platforms());
        }

        MasterProfileDocument.Platforms platforms = master.getPlatforms();

        // Update all platform fields from newest profile
        if (sourcePlatforms.getOs() != null) {
            platforms.setOs(sourcePlatforms.getOs());
        }

        if (sourcePlatforms.getDevice() != null) {
            platforms.setDevice(sourcePlatforms.getDevice());
        }

        if (sourcePlatforms.getBrowser() != null) {
            platforms.setBrowser(sourcePlatforms.getBrowser());
        }

        if (sourcePlatforms.getAppVersion() != null) {
            platforms.setAppVersion(sourcePlatforms.getAppVersion());
        }

        log.debug("  📱 Updated platforms from newest profile");
    }

    private void updateMasterCampaignWithPriority(
            MasterProfileDocument master,
            Profile newestProfile) {

        // ✅ FIX: Use ProfileModel.CampaignModel (interface)
        ProfileModel.CampaignModel sourceCampaign = newestProfile.getCampaign();

        if (sourceCampaign == null) {
            return;
        }

        if (master.getCampaign() == null) {
            master.setCampaign(new MasterProfileDocument.Campaign());
        }

        MasterProfileDocument.Campaign campaign = master.getCampaign();

        // Update all campaign fields from newest profile
        if (sourceCampaign.getUtmSource() != null) {
            campaign.setUtmSource(sourceCampaign.getUtmSource());
        }

        if (sourceCampaign.getUtmCampaign() != null) {
            campaign.setUtmCampaign(sourceCampaign.getUtmCampaign());
        }

        if (sourceCampaign.getUtmMedium() != null) {
            campaign.setUtmMedium(sourceCampaign.getUtmMedium());
        }

        if (sourceCampaign.getUtmContent() != null) {
            campaign.setUtmContent(sourceCampaign.getUtmContent());
        }

        if (sourceCampaign.getUtmTerm() != null) {
            campaign.setUtmTerm(sourceCampaign.getUtmTerm());
        }

        if (sourceCampaign.getUtmCustom() != null) {
            campaign.setUtmCustom(sourceCampaign.getUtmCustom());
        }

        log.debug("  🎯 Updated campaign from newest profile");
    }

    private String mergeMultipleMasters(
            List<MasterProfileDocument> existingMasters,
            List<Profile> newProfiles) {

        log.info("  🔀 Merging {} existing masters", existingMasters.size());

        MasterProfileDocument primaryMaster = existingMasters.stream()
                .min(Comparator.comparing(m -> m.getCreatedAt() != null ? m.getCreatedAt() : Instant.now()))
                .orElse(existingMasters.get(0));

        log.info("  👑 Primary master: {}", primaryMaster.getMasterId());

        Set<String> allProfileIds = new HashSet<>();

        for (MasterProfileDocument master : existingMasters) {
            if (master.getMergedProfileIds() != null) {
                allProfileIds.addAll(master.getMergedProfileIds());
            }
        }

        newProfiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .forEach(allProfileIds::add);

        primaryMaster.setMergedProfileIds(new ArrayList<>(allProfileIds));
        primaryMaster.setMergeCount(allProfileIds.size());

        for (MasterProfileDocument master : existingMasters) {
            if (master.getMasterId().equals(primaryMaster.getMasterId())) {
                continue;
            }
            mergeTraitsFromMaster(primaryMaster, master);
        }

        // ✅ NEW: Calculate newest last_seen_at from all masters + new profiles
        Instant newestLastSeenAt = Stream.concat(
                        existingMasters.stream().map(MasterProfileDocument::getLastSeenAt),
                        newProfiles.stream().map(Profile::getLastSeenAt)
                )
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        primaryMaster.setLastSeenAt(newestLastSeenAt);  // ✅ Update to newest
        primaryMaster.setUpdatedAt(Instant.now());
        primaryMaster.setVersion(
                primaryMaster.getVersion() != null ? primaryMaster.getVersion() + 1 : 1
        );

        MasterProfileDocument saved = masterProfileRepo.save(primaryMaster);

        log.info("  ✅ Primary master saved with {} profiles", saved.getMergedProfileIds().size());

        for (MasterProfileDocument master : existingMasters) {
            if (!master.getMasterId().equals(primaryMaster.getMasterId())) {
                masterProfileRepo.deleteById(master.getId());
                log.info("  🗑️  Deleted master: {}", master.getMasterId());
            }
        }

        markProfilesAsMerged(newProfiles, saved.getMasterId());

        for (MasterProfileDocument deletedMaster : existingMasters) {
            if (!deletedMaster.getMasterId().equals(primaryMaster.getMasterId())) {
                updateProfileReferences(deletedMaster.getMergedProfileIds(), saved.getMasterId());
            }
        }

        return saved.getMasterId();
    }

    private void mergeTraitsFromMaster(
            MasterProfileDocument target,
            MasterProfileDocument source) {

        if (source.getTraits() == null) return;
        if (target.getTraits() == null) {
            target.setTraits(new MasterProfileDocument.Traits());
        }

        MasterProfileDocument.Traits targetTraits = target.getTraits();
        MasterProfileDocument.Traits sourceTraits = source.getTraits();

        if (targetTraits.getFirstName() == null && sourceTraits.getFirstName() != null) {
            targetTraits.setFirstName(sourceTraits.getFirstName());
        }
        if (targetTraits.getLastName() == null && sourceTraits.getLastName() != null) {
            targetTraits.setLastName(sourceTraits.getLastName());
        }
        if (targetTraits.getEmail() == null && sourceTraits.getEmail() != null) {
            targetTraits.setEmail(sourceTraits.getEmail());
        }
        if (targetTraits.getPhone() == null && sourceTraits.getPhone() != null) {
            targetTraits.setPhone(sourceTraits.getPhone());
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
    // MARK PROFILES AS MERGED
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private void markProfilesAsMerged(
            List<Profile> profiles,
            String masterProfileId) {

        for (Profile profile : profiles) {
            try {
                String profileId = ProfileMapper.buildId(
                        profile.getTenantId(),
                        profile.getAppId(),
                        profile.getUserId()
                );

                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);
                if (doc != null) {
                    doc.setStatus("merged");
                    doc.setMergedToMasterId(masterProfileId);
                    doc.setMergedAt(Instant.now());
                    doc.setUpdatedAt(Instant.now());

                    profileRepo.save(doc);
                    log.debug("    ✅ Marked as merged: {}", profileId);
                }
            } catch (Exception ex) {
                log.error("    ❌ Failed to mark profile as merged: {}",
                        profile.getUserId(), ex);
            }
        }
    }

    private void updateProfileReferences(
            List<String> profileIds,
            String newMasterProfileId) {

        if (profileIds == null) return;

        for (String profileId : profileIds) {
            try {
                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);
                if (doc != null) {
                    doc.setStatus("merged");
                    doc.setMergedToMasterId(newMasterProfileId);
                    doc.setMergedAt(Instant.now());
                    doc.setUpdatedAt(Instant.now());

                    profileRepo.save(doc);
                }
            } catch (Exception ex) {
                log.error("Failed to update profile reference: {}", profileId, ex);
            }
        }
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // RESOLVE REMAINING CONFLICTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private int resolveRemainingConflicts(String tenantId) {
        log.info("🔍 Resolving remaining conflicts...");

        List<MasterProfileDocument> allMasters = masterProfileRepo.findByTenantId(tenantId);

        if (allMasters.size() < 2) {
            log.info("  ✅ No conflicts (only {} masters)", allMasters.size());
            return 0;
        }

        Map<String, List<MasterProfileDocument>> groups = new HashMap<>();

        for (MasterProfileDocument master : allMasters) {
            if (master.getTraits() == null) continue;

            if (master.getTraits().getEmail() != null) {
                String key = "email:" + master.getTraits().getEmail();
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(master);
            }

            if (master.getTraits().getPhone() != null) {
                String key = "phone:" + master.getTraits().getPhone();
                groups.computeIfAbsent(key, k -> new ArrayList<>()).add(master);
            }
        }

        int mergedCount = 0;
        Set<String> processed = new HashSet<>();

        for (Map.Entry<String, List<MasterProfileDocument>> entry : groups.entrySet()) {
            List<MasterProfileDocument> conflictingMasters = entry.getValue();

            if (conflictingMasters.size() < 2) continue;

            List<String> masterIds = conflictingMasters.stream()
                    .map(MasterProfileDocument::getMasterId)
                    .distinct()
                    .collect(Collectors.toList());

            if (masterIds.size() < 2) continue;

            String setKey = masterIds.stream().sorted().collect(Collectors.joining("|"));

            if (processed.contains(setKey)) continue;
            processed.add(setKey);

            log.info("  🔀 Merging {} conflicting masters: {}",
                    masterIds.size(), entry.getKey());

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

        List<Profile> profiles = loadProfiles(profileIds);

        if (profiles.isEmpty()) {
            throw new IllegalArgumentException("No profiles found");
        }

        if (profiles.size() < 2) {
            throw new IllegalArgumentException("At least 2 profiles required");
        }

        boolean sameTenant = profiles.stream()
                .allMatch(p -> tenantId.equals(p.getTenantId()));

        if (!sameTenant) {
            throw new IllegalArgumentException("All profiles must belong to same tenant");
        }

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

        MasterProfileDocument doc = masterProfileRepo.findById(
                        ProfileMapper.buildId(tenantId, profiles.get(0).getAppId(), masterProfileId))
                .orElseThrow(() -> new IllegalArgumentException("Master not found"));

        return MasterProfileMapper.toDomain(doc);
    }

    public MasterProfile getMasterProfile(String masterProfileId) {
        // ✅ FIX: Use findByMasterId instead of findAll + filter
        MasterProfileDocument doc = masterProfileRepo.findByMasterId(masterProfileId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Master profile not found: " + masterProfileId));

        log.info("✅ Found master profile: masterId={}, mergedCount={}",
                masterProfileId, doc.getMergeCount());

        return MasterProfileMapper.toDomain(doc);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER METHODS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private List<Profile> loadProfiles(List<String> profileIds) {
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
            Map<String, List<Profile>> groups,
            long startTime) {

        int totalProfiles = groups.values().stream()
                .mapToInt(List::size)
                .sum();

        List<AutoMergeResponse.MergeDetail> details = groups.entrySet().stream()
                .map(entry -> {
                    List<Profile> profiles = entry.getValue();
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
                .masterProfilesCreated(0)
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
        CREATE_NEW,
        UPDATE_EXISTING,
        MERGE_MASTERS
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