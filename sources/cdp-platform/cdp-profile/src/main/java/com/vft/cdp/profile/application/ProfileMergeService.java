package com.vft.cdp.profile.application;

import com.vft.cdp.profile.api.response.AutoMergeResponse;
import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.cache.MasterProfileCacheService;
import com.vft.cdp.profile.infra.cache.ProfileCacheService;
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
import com.vft.cdp.profile.application.dto.MasterProfileDTO;
import com.vft.cdp.profile.application.mapper.MasterProfileDTOMapper;


import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MERGE SERVICE - PURE DOMAIN
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
    private final MasterProfileCacheService masterProfileCache;
    private final ProfileCacheService profileCacheService;

    
    // AUTO MERGE
    

    @Transactional
    public AutoMergeResult autoMerge(
            String tenantId,
            String mergeStrategy,
            Boolean dryRun,
            Integer maxGroups) {

        List<String> createdMasterIds = new ArrayList<>();

        long startTime = System.currentTimeMillis();

        // 1. DETECT DUPLICATES
        Map<String, List<Profile>> duplicateGroups =
                duplicateDetector.findDuplicatesByStrategy(tenantId, mergeStrategy);

        if (duplicateGroups.isEmpty()) {
            log.info(" No duplicates found");
            return buildEmptyResult();
        }

        // Apply max groups limit (Let specific number of group to process)
        Map<String, List<Profile>> groupsToProcess = duplicateGroups;
        if (maxGroups != null && maxGroups > 0) {
            groupsToProcess = duplicateGroups.entrySet().stream()
                    .limit(maxGroups)
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }

        // 2. DRY RUN
        if (Boolean.TRUE.equals(dryRun)) {
            return buildDryRunResult(groupsToProcess, startTime);
        }

        // 3. EXECUTE MERGE
        int mastersCreated = 0;
        int mastersUpdated = 0;
        int profilesMerged = 0;
        List<AutoMergeResponse.MergeDetail> details = new ArrayList<>();

        List<String> allMasterIdsToInvalidate = new ArrayList<>();
        for (Map.Entry<String, List<Profile>> entry : groupsToProcess.entrySet()) {
            String groupKey = entry.getKey();
            List<Profile> profiles = entry.getValue();

            try {

                MergeDecision decision = decideMergeAction(profiles);

                String masterProfileId;

                switch (decision.getAction()) {
                    case CREATE_NEW:
                        masterProfileId = createNewMaster(profiles, groupKey);
                        mastersCreated++;
                        allMasterIdsToInvalidate.add(masterProfileId);
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
                        //Collect IDs of all masters being merged
                        List<String> mergingMasterIds = decision.getExistingMasters()
                                .stream()
                                .map(MasterProfileDocument::getMasterId)
                                .collect(Collectors.toList());

                        allMasterIdsToInvalidate.addAll(mergingMasterIds);

                        masterProfileId = mergeMultipleMasters(
                                decision.getExistingMasters(),
                                profiles
                        );

                        mastersCreated++;
                        allMasterIdsToInvalidate.add(masterProfileId);

                        log.info(" Merged {} masters into: {}",
                                decision.getExistingMasters().size(), masterProfileId);
                        break;

                    default:
                        log.warn("⚠️  Unknown action: {}", decision.getAction());
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
        if (!createdMasterIds.isEmpty()) {
            log.info("Invalidating cache for {} newly created master profiles",
                    createdMasterIds.size());
            masterProfileCache.evictMultiple(createdMasterIds);
        }

        // 4. RESOLVE CONFLICTS
        int mastersMerged = resolveRemainingConflicts(tenantId);

        long duration = System.currentTimeMillis() - startTime;

        log.info("  Duration to merge: {}ms", duration);

        return AutoMergeResult.builder()
                .duplicateGroupsFound(duplicateGroups.size())
                .masterProfilesCreated(mastersCreated + mastersMerged)
                .totalProfilesMerged(profilesMerged)
                .mergeDetails(details)
                .build();
    }

    
    // DECIDE MERGE ACTION
    private MergeDecision decideMergeAction(List<Profile> profiles) {

        List<String> profileIds = profiles.stream()
                .map(p -> ProfileMapper.buildId(p.getTenantId(), p.getAppId(), p.getUserId()))
                .collect(Collectors.toList());

        // STEP 1: Check by merged profile IDs

        Set<MasterProfileDocument> existingMasters = new HashSet<>();

        for (String profileId : profileIds) {
            List<MasterProfileDocument> masters =
                    masterProfileRepo.findByMergedProfileIdsContaining(profileId);
            existingMasters.addAll(masters);
        }

        
        // STEP 2: ALWAYS Check by IDCARD
        //  REMOVED: if (existingMasters.isEmpty())
        

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

        
        // STEP 3: Decide action
        

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

    
    // ACTION 1: CREATE NEW MASTER
    

    private String createNewMaster(List<Profile> profiles, String source) {

        MasterProfile masterProfile = MasterProfileMapper.mergeProfiles(profiles);

        MasterProfileDocument doc = MasterProfileMapper.toDocument(masterProfile);
        MasterProfileDocument saved = masterProfileRepo.save(doc);

        log.info("  💾 New master saved: {}", saved.getMasterId());

        markProfilesAsMerged(profiles, saved.getMasterId());

        return saved.getMasterId();
    }

    
    // ACTION 2: UPDATE EXISTING MASTER
    

    private String updateExistingMaster(
            MasterProfileDocument existingMaster,
            List<Profile> newProfiles) {

        log.info("   Updating master: {}", existingMaster.getMasterId());

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

        
        // Load ALL merged profiles (existing + new)
        

        List<Profile> allMergedProfiles = new ArrayList<>(newProfiles);

        if (!currentMergedIds.isEmpty()) {
            List<Profile> existingProfiles = loadProfiles(currentMergedIds);
            allMergedProfiles.addAll(existingProfiles);
        }
        List<ProfileWithTimestamps> profilesWithTimestamps = allMergedProfiles.stream()
                .map(this::extractTimestampsFromMetadata)
                .collect(Collectors.toList());

        
        // Sort by last_seen_at DESC (newest first)
        

        allMergedProfiles.sort((p1, p2) -> {
            Instant t1 = p1.getLastSeenAt();
            Instant t2 = p2.getLastSeenAt();

            if (t1 == null && t2 == null) return 0;
            if (t1 == null) return 1;   // null last
            if (t2 == null) return -1;  // null last

            return t2.compareTo(t1);  // DESC: newest first
        });

        //Calculate MAX last_seen_at
        Instant maxLastSeenAt = allMergedProfiles.stream()
                .map(Profile::getLastSeenAt)
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());

        //Calculate MIN first_seen_at
        Instant minFirstSeenAt = allMergedProfiles.stream()
                .map(Profile::getFirstSeenAt)
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(Instant.now());

        Profile newestProfile = profilesWithTimestamps.get(0).getProfile();
        Instant masterLastSeenAt = existingMaster.getLastSeenAt();

        updateMasterTraitsFromAllProfiles(
                existingMaster,
                allMergedProfiles,  // Original profiles
                newestProfile,      // Newest by last_seen_at
                masterLastSeenAt    //Pass master's current last_seen_at
        );

        updateMasterPlatformsWithPriority(existingMaster, newestProfile);
        updateMasterCampaignWithPriority(existingMaster, newestProfile);

        //STEP 5: Set timestamps
        existingMaster.setFirstSeenAt(minFirstSeenAt);
        existingMaster.setLastSeenAt(maxLastSeenAt);
        existingMaster.setUpdatedAt(Instant.now());
        existingMaster.setVersion(
                existingMaster.getVersion() != null ? existingMaster.getVersion() + 1 : 1
        );

        MasterProfileDocument saved = masterProfileRepo.save(existingMaster);
        masterProfileCache.evict(saved.getMasterId());
        markProfilesAsMerged(newProfiles, saved.getMasterId());

        return saved.getMasterId();
    }

    @Data
    @AllArgsConstructor
    private static class ProfileWithTimestamps {
        private Profile profile;
        private Instant firstSeenAt;
        private Instant lastSeenAt;
    }

    
    //NEW: Extract timestamps from metadata
    

    private ProfileWithTimestamps extractTimestampsFromMetadata(Profile profile) {
        Instant firstSeenAt = profile.getFirstSeenAt();
        Instant lastSeenAt = profile.getLastSeenAt();

        //Try to extract from metadata if not present in profile
        if (profile.getMetadata() != null) {
            if (firstSeenAt == null && profile.getMetadata().containsKey("first_seen_at")) {
                firstSeenAt = parseTimestamp(profile.getMetadata().get("first_seen_at"));
            }

            if (lastSeenAt == null && profile.getMetadata().containsKey("last_seen_at")) {
                lastSeenAt = parseTimestamp(profile.getMetadata().get("last_seen_at"));
            }
        }

        //Fallback to created_at/updated_at
        if (firstSeenAt == null) {
            firstSeenAt = profile.getCreatedAt();
        }

        if (lastSeenAt == null) {
            lastSeenAt = profile.getUpdatedAt();
        }

        return new ProfileWithTimestamps(profile, firstSeenAt, lastSeenAt);
    }

    
    //NEW: Parse timestamp from various formats
    

    private Instant parseTimestamp(Object obj) {
        if (obj == null) return null;

        if (obj instanceof Instant) {
            return (Instant) obj;
        }

        if (obj instanceof String) {
            try {
                return Instant.parse((String) obj);
            } catch (Exception e) {
                log.warn("Failed to parse timestamp: {}", obj);
                return null;
            }
        }

        if (obj instanceof Long) {
            return Instant.ofEpochMilli((Long) obj);
        }

        if (obj instanceof Integer) {
            return Instant.ofEpochSecond(((Integer) obj).longValue());
        }

        return null;
    }

    private void updateMasterTraitsFromAllProfiles(
            MasterProfileDocument master,
            List<Profile> allProfiles,
            Profile newestProfile,
            Instant masterLastSeenAt) {  //Add parameter

        if (master.getTraits() == null) {
            master.setTraits(new MasterProfileDocument.Traits());
        }

        MasterProfileDocument.Traits traits = master.getTraits();

        // Initialize lists
        if (traits.getEmail() == null) traits.setEmail(new ArrayList<>());
        if (traits.getPhone() == null) traits.setPhone(new ArrayList<>());
        if (traits.getUserId() == null) traits.setUserId(new ArrayList<>());

        
        //STEP 1: Extract timestamps for ALL profiles
        

        Map<String, ProfileWithTimestamps> profileTimestampMap = allProfiles.stream()
                .map(this::extractTimestampsFromMetadata)
                .collect(Collectors.toMap(
                        pwt -> ProfileMapper.buildId(
                                pwt.getProfile().getTenantId(),
                                pwt.getProfile().getAppId(),
                                pwt.getProfile().getUserId()
                        ),
                        pwt -> pwt
                ));

        log.debug("  ⏰ Master current last_seen_at: {}", masterLastSeenAt);

        
        // AGGREGATE LIST FIELDS (no timestamp check needed)
        

        // Emails
        List<String> allEmails = allProfiles.stream()
                .map(Profile::getTraits)
                .filter(Objects::nonNull)
                .map(t -> t.getEmail())
                .filter(Objects::nonNull)
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
                .filter(Objects::nonNull)
                .map(t -> t.getPhone())
                .filter(Objects::nonNull)
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
                .filter(Objects::nonNull)
                .filter(id -> !id.isBlank())
                .collect(Collectors.toSet());

        for (String userId : allUserIds) {
            if (!traits.getUserId().contains(userId)) {
                traits.getUserId().add(userId);
            }
        }

        
        //SINGLE-VALUE FIELDS WITH TIMESTAMP CHECK
        

        // Full Name
        updateSingleValueFieldWithTimestamp(
                traits, "fullName", traits.getFullName(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getFullName() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getFullName()
        );

        // First Name
        updateSingleValueFieldWithTimestamp(
                traits, "firstName", traits.getFirstName(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getFirstName() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getFirstName()
        );

        // Last Name
        updateSingleValueFieldWithTimestamp(
                traits, "lastName", traits.getLastName(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getLastName() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getLastName()
        );

        // Gender
        updateSingleValueFieldWithTimestamp(
                traits, "gender", traits.getGender(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getGender() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getGender()
        );

        // DOB
        updateSingleValueFieldWithTimestamp(
                traits, "dob", traits.getDob(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getDob() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getDob()
        );

        // Address
        updateSingleValueFieldWithTimestamp(
                traits, "address", traits.getAddress(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getAddress() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getAddress()
        );

        // Idcard
        updateSingleValueFieldWithTimestamp(
                traits, "idcard", traits.getIdcard(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getIdcard() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getIdcard()
        );

        // Old Idcard
        updateSingleValueFieldWithTimestamp(
                traits, "oldIdcard", traits.getOldIdcard(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getOldIdcard() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getOldIdcard()
        );

        // Religion
        updateSingleValueFieldWithTimestamp(
                traits, "religion", traits.getReligion(),
                newestProfile.getTraits() != null ? newestProfile.getTraits().getReligion() : null,
                profileTimestampMap, masterLastSeenAt,
                t -> t.getReligion()
        );
    }


    private void updateSingleValueFieldWithTimestamp(
            MasterProfileDocument.Traits traits,
            String fieldName,
            String currentValue,
            String newValue,
            Map<String, ProfileWithTimestamps> profileTimestampMap,
            Instant masterLastSeenAt,
            java.util.function.Function<ProfileModel.TraitsModel, String> fieldExtractor) {

        if (newValue == null || newValue.isBlank()) {
            return; // No new value to set
        }

        // Case 1: Current value is null/blank → Always update
        if (currentValue == null || currentValue.isBlank()) {
            setTraitField(traits, fieldName, newValue);
            log.debug("  Updated {}: {} (was null)", fieldName, newValue);
            return;
        }

        // Case 2: Check if new value comes from a newer profile
        //FIX: Use extracted timestamps from map!
        boolean isFromNewerProfile = profileTimestampMap.values().stream()
                .filter(pwt -> pwt.getProfile().getTraits() != null)
                .filter(pwt -> newValue.equals(fieldExtractor.apply(pwt.getProfile().getTraits())))
                .anyMatch(pwt -> {
                    Instant pLastSeen = pwt.getLastSeenAt();  //Extracted timestamp!

                    if (pLastSeen == null) {
                        log.debug("  ⚠️  Profile has no last_seen_at, skip: {}",
                                pwt.getProfile().getUserId());
                        return false;
                    }

                    if (masterLastSeenAt == null) {
                        log.debug(" Master has no last_seen_at, accept profile: {}",
                                pwt.getProfile().getUserId());
                        return true;
                    }

                    boolean isNewer = !pLastSeen.isBefore(masterLastSeenAt);

                    log.debug("  🔍 Compare {} - profile.last_seen={}, master.last_seen={}, isNewer={}",
                            fieldName, pLastSeen, masterLastSeenAt, isNewer);

                    return isNewer;
                });

        if (isFromNewerProfile) {
            setTraitField(traits, fieldName, newValue);
            log.info(" Updated {}: {} → {} (from newer profile)",
                    fieldName, currentValue, newValue);
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
        List<String> masterIdsToInvalidate = existingMasters.stream()
                .map(MasterProfileDocument::getMasterId)
                .collect(Collectors.toList());

        // Select primary master (oldest one)
        MasterProfileDocument primaryMaster = existingMasters.stream()
                .min(Comparator.comparing(m -> m.getCreatedAt() != null ? m.getCreatedAt() : Instant.now()))
                .orElse(existingMasters.get(0));


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

        Instant maxLastSeenAt = Stream.concat(
                        existingMasters.stream().map(MasterProfileDocument::getLastSeenAt),
                        newProfiles.stream().map(Profile::getLastSeenAt)
                )
                .filter(Objects::nonNull)
                .max(Instant::compareTo)
                .orElse(Instant.now());
        Instant minFirstSeenAt = Stream.concat(
                        existingMasters.stream().map(MasterProfileDocument::getFirstSeenAt),
                        newProfiles.stream().map(Profile::getFirstSeenAt)
                )
                .filter(Objects::nonNull)
                .min(Instant::compareTo)
                .orElse(Instant.now());

        primaryMaster.setFirstSeenAt(minFirstSeenAt);
        primaryMaster.setLastSeenAt(maxLastSeenAt);
        primaryMaster.setUpdatedAt(Instant.now());
        primaryMaster.setVersion(
                primaryMaster.getVersion() != null ? primaryMaster.getVersion() + 1 : 1
        );

        MasterProfileDocument saved = masterProfileRepo.save(primaryMaster);

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

        masterProfileCache.evictMultiple(masterIdsToInvalidate);

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

    
    // MARK PROFILES AS MERGED


    private void markProfilesAsMerged(
            List<Profile> profiles,
            String masterProfileId) {

        if (profiles == null || profiles.isEmpty()) {
            log.debug("⚠️ No profiles to mark as merged");
            return;
        }

        log.info("📝 Marking {} profiles as merged to master: {}",
                profiles.size(), masterProfileId);

        List<String> failedIds = new ArrayList<>();

        for (Profile profile : profiles) {
            String profileId = null;
            try {
                profileId = ProfileMapper.buildId(
                        profile.getTenantId(),
                        profile.getAppId(),
                        profile.getUserId()
                );

                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);

                if (doc == null) {
                    log.warn("⚠️ Profile not found in ES: {}", profileId);
                    continue;
                }

                doc.setStatus("merged");
                doc.setMergedToMasterId(masterProfileId);
                doc.setMergedAt(Instant.now());
                doc.setUpdatedAt(Instant.now());

                profileRepo.save(doc);

                profileCacheService.evict(
                        profile.getTenantId(),
                        profile.getAppId(),
                        profile.getUserId()
                );

                log.debug("Marked as merged: {}", profileId);

            } catch (Exception ex) {
                failedIds.add(profileId != null ? profileId : profile.getUserId());
                log.error("❌ Failed to mark profile as merged: {}",
                        profileId, ex);
            }
        }
    }

    private void updateProfileReferences(
            List<String> profileIds,
            String newMasterProfileId) {

        if (profileIds == null || profileIds.isEmpty()) {
            log.debug("⚠️ No profile references to update");
            return;
        }

        log.info("🔄 Updating {} profile references to master: {}",
                profileIds.size(), newMasterProfileId);

        int successCount = 0;
        int failCount = 0;
        List<String> failedIds = new ArrayList<>();

        for (String profileId : profileIds) {
            try {
                ProfileDocument doc = profileRepo.findById(profileId).orElse(null);

                if (doc == null) {
                    log.warn("Profile not found in ES: {}", profileId);
                    failCount++;
                    failedIds.add(profileId);
                    continue;
                }

                doc.setStatus("merged");
                doc.setMergedToMasterId(newMasterProfileId);
                doc.setMergedAt(Instant.now());
                doc.setUpdatedAt(Instant.now());

                profileRepo.save(doc);

                // ADD THIS: PARSE profileId and EVICT CACHE!
                String[] parts = profileId.split("\\|");
                if (parts.length == 3) {
                    profileCacheService.evict(parts[0], parts[1], parts[2]);
                    log.debug("Evicted cache: {}", profileId);
                } else {
                    log.warn("⚠️ Invalid profileId format: {}", profileId);
                }

                successCount++;
                log.debug("Updated profile reference: {}", profileId);

            } catch (Exception ex) {
                failCount++;
                failedIds.add(profileId);
                log.error("❌ Failed to update profile reference: {}",
                        profileId, ex);
            }
        }

        log.info("📊 Update summary: {} succeeded, ❌ {} failed",
                successCount, failCount);

        // ADD THIS: THROW if any failed
        if (failCount > 0) {
            throw new RuntimeException(
                    String.format("Failed to update %d profile references: %s",
                            failCount, failedIds)
            );
        }
    }

    
    // RESOLVE REMAINING CONFLICTS
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

    
    // MANUAL MERGE
    

    @Transactional
    public MasterProfileDTO manualMerge(
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

        MasterProfile masterProfile = MasterProfileMapper.toDomain(doc);
        MasterProfileDTO dto = MasterProfileDTOMapper.toDTO(masterProfile);

        
        //NEW: Cache the newly created master profile
        

        masterProfileCache.put(masterProfile.getProfileId(), dto);
        log.info("Cached newly merged master profile: masterId={}", masterProfile.getProfileId());

        return dto;
    }

    public MasterProfileDTO getMasterProfile(String masterProfileId) {
        Optional<MasterProfileDTO> cached = masterProfileCache.get(masterProfileId);
        if (cached.isPresent()) {
            log.info("Cache HIT: masterId={}", masterProfileId);
            return cached.get();
        }
        log.debug("❌ Cache MISS: masterId={}, loading from ES", masterProfileId);

        //  FIX: Use findByMasterId instead of findAll + filter
        MasterProfileDocument doc = masterProfileRepo.findByMasterId(masterProfileId)
                .orElseThrow(() -> new IllegalArgumentException(
                        "Master profile not found: " + masterProfileId));

        log.info(" Found master profile: masterId={}, mergedCount={}",
                masterProfileId, doc.getMergeCount());

        MasterProfile masterProfile = MasterProfileMapper.toDomain(doc);
        MasterProfileDTO dto = MasterProfileDTOMapper.toDTO(masterProfile);

        // STEP 4: Cache the result (L1 + L2)
        

        masterProfileCache.put(masterProfileId, dto);
        log.info("Cached master profile: masterId={}", masterProfileId);
        return dto;
    }

    
    // HELPER METHODS

    private List<Profile> loadProfiles(List<String> profileIds) {
        if (profileIds == null || profileIds.isEmpty()) {
            return new ArrayList<>();
        }

        log.debug("📥 Loading {} profiles (with cache)", profileIds.size());

        List<Profile> profiles = new ArrayList<>();
        int cacheHits = 0;
        int cacheMisses = 0;

        for (String profileId : profileIds) {
            try {
                // Parse profileId: "tenantId|appId|userId"
                String[] parts = profileId.split("\\|");
                if (parts.length != 3) {
                    log.warn("⚠️  Invalid profileId format: {}", profileId);
                    continue;
                }

                String tenantId = parts[0];
                String appId = parts[1];
                String userId = parts[2];

                // STEP 1: Check cache first
                Optional<com.vft.cdp.profile.application.model.ProfileModel> cachedOpt =
                        profileCacheService.get(tenantId, appId, userId);

                if (cachedOpt.isPresent()) {
                    // Cache HIT - convert to domain
                    cacheHits++;
                    Profile profile = convertToDomain(cachedOpt.get());
                    profiles.add(profile);

                    log.trace("  Cache HIT: {}", profileId);

                } else {
                    // ❌ Cache MISS - load from ES
                    cacheMisses++;

                    com.vft.cdp.profile.infra.es.document.ProfileDocument doc =
                            profileRepo.findById(profileId).orElse(null);

                    if (doc != null) {
                        Profile profile = com.vft.cdp.profile.infra.es.mapper.ProfileMapper.toDomain(doc);
                        profiles.add(profile);

                        // Populate cache for future use
                        profileCacheService.put(tenantId, appId, userId, profile);

                        log.trace("  ❌ Cache MISS (populated): {}", profileId);
                    } else {
                        log.warn("  ⚠️  Profile not found in ES: {}", profileId);
                    }
                }

            } catch (Exception ex) {
                log.error("❌ Failed to load profile: {}", profileId, ex);
            }
        }

        // Log cache performance stats
        int total = cacheHits + cacheMisses;
        if (total > 0) {
            double hitRate = (cacheHits * 100.0) / total;
            log.info("📊 Profile Load Stats: {} total, {}  hits ({:.1f}%), ❌ {} misses",
                    total, cacheHits, hitRate, cacheMisses);

            // ⚠️ Alert if cache hit rate is low
            if (hitRate < 50.0 && total > 10) {
                log.warn("⚠️  Low cache hit rate ({:.1f}%) - consider warming up cache before merge", hitRate);
            }
        }

        return profiles;
    }
    /**
     * Helper: Convert ProfileModel to Domain Profile
     */
    private Profile convertToDomain(com.vft.cdp.profile.application.model.ProfileModel model) {
        if (model instanceof Profile) {
            return (Profile) model;
        }

        return Profile.builder()
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())
                .status(com.vft.cdp.profile.domain.ProfileStatus.fromValue(model.getStatus()))
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

    
    // DATA CLASSES
    

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