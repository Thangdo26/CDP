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
 *  NO EnrichedProfile - uses Domain Profile only
 *  GUARANTEES:
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
            log.info(" No duplicates found");
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
                        log.info("   Created new master: {}", masterProfileId);
                        break;

                    case UPDATE_EXISTING:
                        masterProfileId = updateExistingMaster(
                                decision.getExistingMaster(),
                                profiles
                        );
                        mastersUpdated++;
                        log.info("   Updated existing master: {}", masterProfileId);
                        break;

                    case MERGE_MASTERS:
                        masterProfileId = mergeMultipleMasters(
                                decision.getExistingMasters(),
                                profiles
                        );
                        mastersCreated++;
                        log.info("   Merged {} masters into: {}",
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
        log.info(" AUTO MERGE COMPLETED");
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
        // STEP 1: Check by merged profile IDs
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Set<MasterProfileDocument> existingMasters = new HashSet<>();

        for (String profileId : profileIds) {
            List<MasterProfileDocument> masters =
                    masterProfileRepo.findByMergedProfileIdsContaining(profileId);
            existingMasters.addAll(masters);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 2: ALWAYS Check by IDCARD
        //  REMOVED: if (existingMasters.isEmpty())
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Set<String> idcards = profiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(traits -> traits.getIdcard())
                .filter(Objects::nonNull)
                .filter(idcard -> !idcard.isBlank())
                .collect(Collectors.toSet());

        for (String idcard : idcards) {
            List<MasterProfileDocument> mastersByIdcard =
                    masterProfileRepo.findByTraitsIdcard(idcard);

            if (!mastersByIdcard.isEmpty()) {
                log.info("  🎯 Found {} existing master(s) with same idcard: {}",
                        mastersByIdcard.size(), idcard);
                existingMasters.addAll(mastersByIdcard);
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 3: Decide action
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
        // Load ALL merged profiles (existing + new)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        List<Profile> allMergedProfiles = new ArrayList<>(newProfiles);

        if (!currentMergedIds.isEmpty()) {
            List<Profile> existingProfiles = loadProfiles(currentMergedIds);
            allMergedProfiles.addAll(existingProfiles);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Sort by last_seen_at DESC (newest first)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        allMergedProfiles.sort((p1, p2) -> {
            Instant t1 = p1.getLastSeenAt();
            Instant t2 = p2.getLastSeenAt();

            if (t1 == null && t2 == null) return 0;
            if (t1 == null) return 1;   // null last
            if (t2 == null) return -1;  // null last

            return t2.compareTo(t1);  // DESC: newest first
        });

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ✅ Calculate MAX last_seen_at from ALL profiles
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Instant maxLastSeenAt = allMergedProfiles.stream()
                .map(Profile::getLastSeenAt)
                .filter(java.util.Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        log.info("  📅 Max last_seen_at from {} profiles: {}",
                allMergedProfiles.size(), maxLastSeenAt);

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // Update master with ALL profiles
        // allProfiles already sorted by last_seen_at DESC
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        updateMasterTraitsFromAllProfiles(existingMaster, allMergedProfiles, allMergedProfiles.get(0));
        updateMasterPlatformsWithPriority(existingMaster, allMergedProfiles.get(0));
        updateMasterCampaignWithPriority(existingMaster, allMergedProfiles.get(0));

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ✅ Set last_seen_at to MAX value
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        existingMaster.setLastSeenAt(maxLastSeenAt);

        // Always update metadata
        existingMaster.setUpdatedAt(Instant.now());
        existingMaster.setVersion(
                existingMaster.getVersion() != null ? existingMaster.getVersion() + 1 : 1
        );

        MasterProfileDocument saved = masterProfileRepo.save(existingMaster);

        log.info("  ✅ Master updated: {} profiles, version={}, last_seen_at={}",
                saved.getMergedProfileIds().size(),
                saved.getVersion(),
                saved.getLastSeenAt());

        markProfilesAsMerged(newProfiles, saved.getMasterId());

        return saved.getMasterId();
    }

    private void updateMasterTraitsFromAllProfiles(
            MasterProfileDocument master,
            List<Profile> allProfiles,
            Profile newestProfile) {

        if (master.getTraits() == null) {
            master.setTraits(new MasterProfileDocument.Traits());
        }

        MasterProfileDocument.Traits traits = master.getTraits();

        // Initialize lists
        if (traits.getEmail() == null) traits.setEmail(new ArrayList<>());
        if (traits.getPhone() == null) traits.setPhone(new ArrayList<>());
        if (traits.getUserId() == null) traits.setUserId(new ArrayList<>());

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // AGGREGATE LIST FIELDS (no timestamp check needed)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        // Emails
        List<String> allEmails = allProfiles.stream()
                .map(Profile::getTraits)
                .filter(java.util.Objects::nonNull)
                .map(t -> t.getEmail())
                .filter(java.util.Objects::nonNull)
                .filter(e -> !e.isBlank())
                .map(String::trim)
                .collect(Collectors.toList());

        for (String email : allEmails) {
            boolean exists = traits.getEmail().stream()
                    .anyMatch(existing -> existing.equalsIgnoreCase(email));
            if (!exists) {
                traits.getEmail().add(email);
            }
        }

        // Phones
        Set<String> allPhones = allProfiles.stream()
                .map(Profile::getTraits)
                .filter(java.util.Objects::nonNull)
                .map(t -> t.getPhone())
                .filter(java.util.Objects::nonNull)
                .filter(p -> !p.isBlank())
                .map(String::trim)
                .collect(Collectors.toSet());

        for (String phone : allPhones) {
            if (!traits.getPhone().contains(phone)) {
                traits.getPhone().add(phone);
            }
        }

        // User IDs
        Set<String> allUserIds = allProfiles.stream()
                .map(Profile::getUserId)
                .filter(java.util.Objects::nonNull)
                .filter(id -> !id.isBlank())
                .collect(Collectors.toSet());

        for (String userId : allUserIds) {
            if (!traits.getUserId().contains(userId)) {
                traits.getUserId().add(userId);
            }
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ✅ SINGLE-VALUE FIELDS WITH TIMESTAMP CHECK
        // Only update if profile's last_seen_at >= master's last_seen_at
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Instant masterLastSeenAt = master.getLastSeenAt();
        log.debug("  ⏰ Master current last_seen_at: {}", masterLastSeenAt);

        // Full Name
        String newFullName = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getFullName());
        updateSingleValueField(traits, "fullName", traits.getFullName(),
                newFullName, allProfiles, masterLastSeenAt, t -> t.getFullName());

        // First Name
        String newFirstName = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getFirstName());
        updateSingleValueField(traits, "firstName", traits.getFirstName(),
                newFirstName, allProfiles, masterLastSeenAt, t -> t.getFirstName());

        // Last Name
        String newLastName = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getLastName());
        updateSingleValueField(traits, "lastName", traits.getLastName(),
                newLastName, allProfiles, masterLastSeenAt, t -> t.getLastName());

        // Gender
        String newGender = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getGender());
        updateSingleValueField(traits, "gender", traits.getGender(),
                newGender, allProfiles, masterLastSeenAt, t -> t.getGender());

        // DOB
        String newDob = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getDob());
        updateSingleValueField(traits, "dob", traits.getDob(),
                newDob, allProfiles, masterLastSeenAt, t -> t.getDob());

        // Address
        String newAddress = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getAddress());
        updateSingleValueField(traits, "address", traits.getAddress(),
                newAddress, allProfiles, masterLastSeenAt, t -> t.getAddress());

        // Idcard
        String newIdcard = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getIdcard());
        updateSingleValueField(traits, "idcard", traits.getIdcard(),
                newIdcard, allProfiles, masterLastSeenAt, t -> t.getIdcard());

        // Old Idcard
        String newOldIdcard = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getOldIdcard());
        updateSingleValueField(traits, "oldIdcard", traits.getOldIdcard(),
                newOldIdcard, allProfiles, masterLastSeenAt, t -> t.getOldIdcard());

        // Religion
        String newReligion = getFirstValueFromNewerProfiles(
                allProfiles, masterLastSeenAt, t -> t.getReligion());
        updateSingleValueField(traits, "religion", traits.getReligion(),
                newReligion, allProfiles, masterLastSeenAt, t -> t.getReligion());

    }

    private void updateSingleValueField(
            MasterProfileDocument.Traits traits,
            String fieldName,
            String currentValue,
            String newValue,
            List<Profile> allProfiles,
            Instant masterLastSeenAt,
            java.util.function.Function<ProfileModel.TraitsModel, String> fieldExtractor) {

        if (newValue == null || newValue.isBlank()) {
            return; // No new value to set
        }

        // Case 1: Current value is null/blank → Always update
        if (currentValue == null || currentValue.isBlank()) {
            setTraitField(traits, fieldName, newValue);
            log.debug("  📝 Updated {}: {} (was null)", fieldName, newValue);
            return;
        }

        // Case 2: Check if new value comes from a newer profile
        boolean isFromNewerProfile = allProfiles.stream()
                .filter(p -> p.getTraits() != null)
                .filter(p -> newValue.equals(fieldExtractor.apply(p.getTraits())))
                .anyMatch(p -> {
                    Instant pLastSeen = p.getLastSeenAt();
                    return pLastSeen != null &&
                            (masterLastSeenAt == null || !pLastSeen.isBefore(masterLastSeenAt));
                });

        if (isFromNewerProfile) {
            setTraitField(traits, fieldName, newValue);
            log.debug("  📝 Updated {}: {} (from newer profile)", fieldName, newValue);
        } else {
            log.debug("  ⏭️  Skipped {}: profile too old (last_seen_at < master)", fieldName);
        }
    }

    /**
     * Set trait field by name using reflection-like approach
     */
    private void setTraitField(MasterProfileDocument.Traits traits, String fieldName, String value) {
        switch (fieldName) {
            case "fullName": traits.setFullName(value); break;
            case "firstName": traits.setFirstName(value); break;
            case "lastName": traits.setLastName(value); break;
            case "gender": traits.setGender(value); break;
            case "dob": traits.setDob(value); break;
            case "address": traits.setAddress(value); break;
            case "idcard": traits.setIdcard(value); break;
            case "oldIdcard": traits.setOldIdcard(value); break;
            case "religion": traits.setReligion(value); break;
        }
    }

    /**
     * Get first non-null value from profiles with last_seen_at >= masterLastSeenAt
     */
    private String getFirstValueFromNewerProfiles(
            List<Profile> allProfiles,
            Instant masterLastSeenAt,
            java.util.function.Function<ProfileModel.TraitsModel, String> fieldExtractor) {

        return allProfiles.stream()
                .filter(p -> {
                    Instant pLastSeen = p.getLastSeenAt();
                    return pLastSeen != null &&
                            (masterLastSeenAt == null || !pLastSeen.isBefore(masterLastSeenAt));
                })
                .map(Profile::getTraits)
                .filter(java.util.Objects::nonNull)
                .map(fieldExtractor)
                .filter(java.util.Objects::nonNull)
                .filter(s -> !s.isBlank())
                .findFirst()
                .orElse(null);
    }


    /*
     *  NEW: Update master platforms from newest profile
     */
    private void updateMasterPlatformsWithPriority(
            MasterProfileDocument master,
            Profile newestProfile) {

        //  FIX: Use ProfileModel.PlatformsModel (interface)
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

        //  FIX: Use ProfileModel.CampaignModel (interface)
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

        // Select primary master (oldest one)
        MasterProfileDocument primaryMaster = existingMasters.stream()
                .min(Comparator.comparing(m -> m.getCreatedAt() != null ? m.getCreatedAt() : Instant.now()))
                .orElse(existingMasters.get(0));

        log.info("  👑 Primary master: {}", primaryMaster.getMasterId());

        // Collect all profile IDs
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

        // Merge traits from other masters
        for (MasterProfileDocument master : existingMasters) {
            if (master.getMasterId().equals(primaryMaster.getMasterId())) {
                continue;
            }
            mergeTraitsFromMaster(primaryMaster, master);
        }

        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // ✅ Calculate MAX last_seen_at from masters + profiles
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

        Instant maxLastSeenAt = Stream.concat(
                        existingMasters.stream().map(MasterProfileDocument::getLastSeenAt),
                        newProfiles.stream().map(Profile::getLastSeenAt)
                )
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        log.info("  📅 Max last_seen_at from {} masters + {} profiles: {}",
                existingMasters.size(), newProfiles.size(), maxLastSeenAt);

        primaryMaster.setLastSeenAt(maxLastSeenAt);
        primaryMaster.setUpdatedAt(Instant.now());
        primaryMaster.setVersion(
                primaryMaster.getVersion() != null ? primaryMaster.getVersion() + 1 : 1
        );

        MasterProfileDocument saved = masterProfileRepo.save(primaryMaster);

        log.info("  ✅ Primary master saved with {} profiles, last_seen_at={}",
                saved.getMergedProfileIds().size(),
                saved.getLastSeenAt());

        // Delete other masters
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
                    log.debug("     Marked as merged: {}", profileId);
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
            log.info("   No conflicts (only {} masters)", allMasters.size());
            return 0;
        }

        Map<String, List<MasterProfileDocument>> groups = new HashMap<>();

        for (MasterProfileDocument master : allMasters) {
            if (master.getTraits() == null) continue;

            if (master.getTraits().getEmail() != null && !master.getTraits().getEmail().isEmpty()) {
                for (String email : master.getTraits().getEmail()) {
                    String key = "email:" + email;
                    groups.computeIfAbsent(key, k -> new ArrayList<>()).add(master);
                }
            }

            if (master.getTraits().getPhone() != null && !master.getTraits().getPhone().isEmpty()) {
                for (String phone : master.getTraits().getPhone()) {
                    String key = "phone:" + phone;
                    groups.computeIfAbsent(key, k -> new ArrayList<>()).add(master);
                }
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

        log.info("   Resolved {} conflicts", mergedCount);
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
        //  FIX: Use findByMasterId instead of findAll + filter
        MasterProfileDocument doc = masterProfileRepo.findByMasterId(masterProfileId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Master profile not found: " + masterProfileId));

        log.info(" Found master profile: masterId={}, mergedCount={}",
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