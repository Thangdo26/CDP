package com.vft.cdp.profile.application;

import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.application.repository.ProfileMappingRepository;
import com.vft.cdp.profile.application.repository.ProfileRepository;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import com.vft.cdp.profile.infra.cache.ProfileCacheService;
import com.vft.cdp.profile.application.mapper.ProfileDTOMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE TRACK SERVICE - FIXED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * FIXES:
 * 1. Correct profileId generation and storage
 * 2. Cache AFTER merge/track completes
 * 3. Proper mapping creation with correct profileId
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileTrackService {

    private final ProfileMappingRepository mappingRepository;
    private final ProfileRepository profileRepository;
    private final ProfileCacheService cacheService;

    /**
     * Process incoming profile track request
     */
    public ProcessResult processTrack(CreateProfileCommand command) {
        String tenantId = command.getTenantId();
        String appId = command.getAppId();
        String userId = command.getUserId();
        String idcard = command.getTraits() != null ? command.getTraits().getIdcard() : null;

        log.info("🔥 Processing track: tenant={}, app={}, user={}, idcard={}",
                tenantId, appId, userId, idcard);

        Instant incomingUpdatedAt = extractUpdatedAt(command.getMetadata());

        // ═══════════════════════════════════════════════════════════════
        // STEP 1: Check profile_mapping
        // ═══════════════════════════════════════════════════════════════
        Optional<String> existingProfileId = mappingRepository.findProfileId(tenantId, appId, userId);

        if (existingProfileId.isPresent()) {
            return handleExistingMapping(existingProfileId.get(), command, incomingUpdatedAt);
        }

        // ═══════════════════════════════════════════════════════════════
        // STEP 2: Merge Service - Find by idcard
        // ═══════════════════════════════════════════════════════════════
        return handleMergeService(command, incomingUpdatedAt);
    }

    /**
     * Handle existing mapping
     */
    private ProcessResult handleExistingMapping(
            String profileId,
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        log.info("📋 Found existing mapping → profile_id={}", profileId);

        Optional<ProfileModel> profileOpt = profileRepository.findById(profileId);

        if (profileOpt.isEmpty()) {
            log.warn("⚠️ Profile not found for mapping! Creating new...");
            return createNewProfile(command);
        }

        ProfileModel existingProfile = profileOpt.get();
        Instant existingUpdatedAt = existingProfile.getUpdatedAt();

        if (shouldUpdate(incomingUpdatedAt, existingUpdatedAt)) {
            log.info("✅ Incoming data is newer, updating profile");
            return updateProfile(existingProfile, command, incomingUpdatedAt);
        } else {
            log.info("⏭️ Incoming data is older/same, skipping update");
            return ProcessResult.skipped(profileId, "Incoming updated_at <= existing updated_at");
        }
    }

    /**
     * Handle Merge Service
     */
    private ProcessResult handleMergeService(
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        String idcard = command.getTraits() != null ? command.getTraits().getIdcard() : null;

        if (idcard == null || idcard.isBlank()) {
            log.info("🆕 No idcard, creating new profile");
            return createNewProfile(command);
        }

        log.info("🔀 Merge Service: idcard={}", idcard);

        // ✅ Step 1: Find profiles with this idcard
        List<ProfileModel> matchingProfiles = profileRepository.findByIdcard(
                command.getTenantId(),
                idcard
        );

        if (matchingProfiles.isEmpty()) {
            log.info("🆕 No existing profile with idcard={}, creating new", idcard);
            return createNewProfile(command);
        }

        // ✅ Step 2: Get existing profile
        ProfileModel existingProfile = matchingProfiles.get(0);
        String documentId = extractProfileId(existingProfile);
        Instant existingUpdatedAt = existingProfile.getUpdatedAt();

        log.info("🔍 Found existing profile: documentId={}, idcard={}", documentId, idcard);

        // ✅ Step 3: Load FRESH profile from ES to get current users list
        Optional<ProfileModel> freshProfileOpt = profileRepository.findById(documentId);

        if (freshProfileOpt.isEmpty()) {
            log.warn("⚠️ Profile disappeared during merge");
            return createNewProfile(command);
        }

        Profile profile = convertToDomain(freshProfileOpt.get());

        log.info("📋 Current users in profile: {}",
                profile.getUsers() != null ? profile.getUsers().size() : 0);

        // ✅ Step 4: Check if this user already exists
        boolean userExists = profile.hasUser(command.getAppId(), command.getUserId());

        if (userExists) {
            log.info("⏭️ User already linked to this profile: {}|{}",
                    command.getAppId(), command.getUserId());

            // Just update data if newer
            if (shouldUpdate(incomingUpdatedAt, existingUpdatedAt)) {
                return updateProfile(profile, command, incomingUpdatedAt);
            } else {
                return ProcessResult.skipped(documentId, "User already linked, data not newer");
            }
        }

        // ✅ Step 5: Add new user to profile
        log.info("➕ Adding new user to profile: {}|{}", command.getAppId(), command.getUserId());

        profile.addUser(command.getAppId(), command.getUserId());

        log.info("✅ Users after add: {}", profile.getUsers().size());

        // ✅ Step 6: Update data if newer
        if (shouldUpdate(incomingUpdatedAt, existingUpdatedAt)) {
            log.info("🔄 Updating profile data (newer timestamp)");

            profile.update(
                    mapTraits(command.getTraits()),
                    mapPlatforms(command.getPlatforms()),
                    mapCampaign(command.getCampaign())
            );

            if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
                profile.updateMetadata(command.getMetadata());
            }

            if (incomingUpdatedAt != null) {
                profile.setUpdatedAt(incomingUpdatedAt);
            } else {
                profile.setUpdatedAt(Instant.now());
            }
        } else {
            log.info("⏭️ Keeping existing data (newer or same timestamp)");
        }

        // ✅ Step 7: Save updated profile
        ProfileModel saved = profileRepository.save(profile);

        log.info("💾 Profile saved: documentId={}, users.size={}",
                documentId, profile.getUsers().size());

        // ✅ Step 8: Create mapping with LINKING ID (idcard)
        mappingRepository.saveMapping(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                idcard  // ✅ Use idcard as profile_id
        );

        log.info("🔗 New mapping created: {}|{}|{} -> idcard={}",
                command.getTenantId(), command.getAppId(), command.getUserId(), idcard);

        // ✅ Step 9: Invalidate cache for ALL linked users
        invalidateCacheForProfile(documentId, command.getTenantId(),
                command.getAppId(), command.getUserId());

        // ✅ Step 10: Cache new data
        cacheService.put(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                saved
        );

        ProcessResult result = ProcessResult.updated(documentId, ProfileDTOMapper.toDTO(saved));
        result.setMappingCreated(true);

        log.info("✅ Merge complete: documentId={}, total users={}",
                documentId, profile.getUsers().size());

        return result;
    }

    private String generateDocumentId() {
        return UUID.randomUUID().toString();
    }

    private ProcessResult createNewProfile(CreateProfileCommand command) {

        // ✅ Step 1: Generate DOCUMENT ID (UUID for ES _id)
        String documentId = UUID.randomUUID().toString();

        // ✅ Step 2: Extract LINKING ID (idcard for profile_mapping.profile_id)
        String linkingId = extractProfileLinkingId(command);

        log.info("🆕 Creating new profile: documentId={}, linkingId={}", documentId, linkingId);

        Instant now = Instant.now();

        // ✅ Build domain with documentId as userId (ES _id)
        Profile newProfile = Profile.builder()
                .tenantId(command.getTenantId())
                .appId(command.getAppId())
                .userId(documentId)  // ✅ CRITICAL: Use UUID here
                .type(command.getType())
                .status(ProfileStatus.ACTIVE)
                .users(new ArrayList<>())  // ✅ Initialize empty list
                .traits(mapTraits(command.getTraits()))
                .platforms(mapPlatforms(command.getPlatforms()))
                .campaign(mapCampaign(command.getCampaign()))
                .metadata(command.getMetadata() != null ? command.getMetadata() : new HashMap<>())
                .createdAt(now)
                .updatedAt(now)
                .firstSeenAt(now)
                .lastSeenAt(now)
                .version(1)
                .build();

        // ✅ Add first user
        newProfile.addUser(command.getAppId(), command.getUserId());

        log.debug("✅ Added user to new profile: {}|{}", command.getAppId(), command.getUserId());

        // ✅ Save to ES with UUID as _id
        ProfileModel saved = profileRepository.save(newProfile);

        log.info("💾 Profile saved to ES: _id={}", documentId);

        // ✅ Create mapping with LINKING ID (idcard)
        mappingRepository.saveMapping(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                linkingId  // ✅ Use idcard as profile_id
        );

        log.info("🔗 Mapping created: {}|{}|{} -> linkingId={}",
                command.getTenantId(), command.getAppId(), command.getUserId(), linkingId);

        // Cache
        cacheService.put(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                saved
        );

        log.info("✅ Profile created successfully: documentId={}, users.size={}",
                documentId, newProfile.getUsers().size());

        return ProcessResult.created(documentId, ProfileDTOMapper.toDTO(saved));
    }

    /**
     * ✅ Extract profile linking ID (idcard) for profile_mapping
     */
    private String extractProfileLinkingId(CreateProfileCommand command) {
        if (command.getTraits() != null && command.getTraits().getIdcard() != null) {
            String idcard = command.getTraits().getIdcard();
            if (!idcard.isBlank()) {
                return idcard;  // ✅ Return RAW idcard (no prefix)
            }
        }
        // Fallback: use UUID if no idcard
        return UUID.randomUUID().toString();
    }

    private ProcessResult updateProfile(
            ProfileModel existingModel,
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        String profileId = extractProfileId(existingModel);

        Profile profile = convertToDomain(existingModel);

        // ✅ Step 1: Update data (this does NOT touch updatedAt)
        profile.update(
                mapTraits(command.getTraits()),
                mapPlatforms(command.getPlatforms()),
                mapCampaign(command.getCampaign())
        );

        // ✅ Step 2: Update metadata
        if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
            profile.updateMetadata(command.getMetadata());
        }

        // ✅ Step 3: Set updatedAt from incoming metadata (AFTER update())
        if (incomingUpdatedAt != null) {
            profile.setUpdatedAt(incomingUpdatedAt);
            log.debug("⏰ Set updated_at from metadata: {}", incomingUpdatedAt);
        } else {
            profile.setUpdatedAt(Instant.now());
            log.debug("⏰ Set updated_at to now (no metadata timestamp)");
        }

        // ✅ Step 4: Save to ES
        ProfileModel saved = profileRepository.save(profile);

        // ✅ ADD THIS: Invalidate cache for ALL users linked to this profile
        invalidateCacheForProfile(profileId, command.getTenantId(), command.getAppId(), command.getUserId());


        // ✅ Step 5: Cache AFTER save
        cacheService.put(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                saved
        );

        return ProcessResult.updated(profileId, ProfileDTOMapper.toDTO(saved));
    }

    // ═══════════════════════════════════════════════════════════════
    // HELPER METHODS
    // ═══════════════════════════════════════════════════════════════

    private void invalidateCacheForProfile(
            String profileId,
            String currentTenantId,
            String currentAppId,
            String currentUserId
    ) {
        // Evict current user first
        cacheService.evict(currentTenantId, currentAppId, currentUserId);

        // Get all mappings for this profile
        List<String> allMappings = mappingRepository.findMappingsByProfileId(profileId);

        // Evict cache for each linked user
        for (String mappingId : allMappings) {
            String[] parts = mappingId.split("\\|");
            if (parts.length == 3) {
                String t = parts[0];
                String a = parts[1];
                String u = parts[2];

                // Skip current user (already evicted)
                if (!t.equals(currentTenantId) || !a.equals(currentAppId) || !u.equals(currentUserId)) {
                    cacheService.evict(t, a, u);
                    log.debug("🗑️ Cache evicted for linked user: {}|{}|{}", t, a, u);
                }
            }
        }

        log.info("✅ Cache invalidated for all {} linked users of profile {}",
                allMappings.size(), profileId);
    }

    /**
     * ✅ FIXED: Proper profileId generation
     */
    private String generateProfileId() {
        return UUID.randomUUID().toString();
    }

    /**
     * ✅ FIXED: Extract profileId from ProfileModel
     */
    private String extractProfileId(ProfileModel model) {
        // DocumentId is stored in userId field
        String userId = model.getUserId();

        if (userId != null && !userId.isBlank()) {
            return userId;  // ✅ Return UUID directly
        }

        // This should NOT happen
        log.warn("⚠️ ProfileModel.userId is null!");
        return UUID.randomUUID().toString();
    }

    private Instant extractUpdatedAt(Map<String, Object> metadata) {
        if (metadata == null) return Instant.now();

        Object obj = metadata.get("updated_at");
        if (obj == null) return Instant.now();

        if (obj instanceof Instant) return (Instant) obj;
        if (obj instanceof String) {
            try {
                return Instant.parse((String) obj);
            } catch (Exception e) {
                log.warn("Failed to parse updated_at: {}", obj);
            }
        }
        if (obj instanceof Long) return Instant.ofEpochMilli((Long) obj);

        return Instant.now();
    }

    private boolean shouldUpdate(Instant incoming, Instant existing) {
        if (incoming == null) return true;
        if (existing == null) return true;
        return incoming.isAfter(existing);
    }

    private Profile convertToDomain(ProfileModel model) {
        if (model instanceof Profile) return (Profile) model;

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

    private Profile.Traits mapTraits(CreateProfileCommand.TraitsCommand cmd) {
        if (cmd == null) return null;
        return Profile.Traits.builder()
                .fullName(cmd.getFullName())
                .firstName(cmd.getFirstName())
                .lastName(cmd.getLastName())
                .idcard(cmd.getIdcard())
                .oldIdcard(cmd.getOldIdcard())
                .phone(cmd.getPhone())
                .email(cmd.getEmail())
                .gender(cmd.getGender())
                .dob(cmd.getDob())
                .address(cmd.getAddress())
                .religion(cmd.getReligion())
                .build();
    }

    private Profile.Platforms mapPlatforms(CreateProfileCommand.PlatformsCommand cmd) {
        if (cmd == null) return null;
        return Profile.Platforms.builder()
                .os(cmd.getOs())
                .device(cmd.getDevice())
                .browser(cmd.getBrowser())
                .appVersion(cmd.getAppVersion())
                .build();
    }

    private Profile.Campaign mapCampaign(CreateProfileCommand.CampaignCommand cmd) {
        if (cmd == null) return null;
        return Profile.Campaign.builder()
                .utmSource(cmd.getUtmSource())
                .utmCampaign(cmd.getUtmCampaign())
                .utmMedium(cmd.getUtmMedium())
                .utmContent(cmd.getUtmContent())
                .utmTerm(cmd.getUtmTerm())
                .utmCustom(cmd.getUtmCustom())
                .build();
    }

    private Profile.Traits convertTraits(ProfileModel.TraitsModel traits) {
        if (traits == null) return null;
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

    private Profile.Platforms convertPlatforms(ProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;
        return Profile.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private Profile.Campaign convertCampaign(ProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;
        return Profile.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    // ═══════════════════════════════════════════════════════════════
    // RESULT CLASS
    // ═══════════════════════════════════════════════════════════════

    @lombok.Data
    @lombok.Builder
    @lombok.AllArgsConstructor
    public static class ProcessResult {
        private Action action;
        private String profileId;
        private String message;
        private ProfileDTO profile;
        private boolean mappingCreated;

        public enum Action {
            CREATED, UPDATED, SKIPPED, MAPPING_ONLY
        }

        public static ProcessResult created(String profileId, ProfileDTO profile) {
            return ProcessResult.builder()
                    .action(Action.CREATED)
                    .profileId(profileId)
                    .message("Profile created successfully")
                    .profile(profile)
                    .mappingCreated(true)
                    .build();
        }

        public static ProcessResult updated(String profileId, ProfileDTO profile) {
            return ProcessResult.builder()
                    .action(Action.UPDATED)
                    .profileId(profileId)
                    .message("Profile updated successfully")
                    .profile(profile)
                    .mappingCreated(false)
                    .build();
        }

        public static ProcessResult skipped(String profileId, String reason) {
            return ProcessResult.builder()
                    .action(Action.SKIPPED)
                    .profileId(profileId)
                    .message(reason)
                    .profile(null)
                    .mappingCreated(false)
                    .build();
        }

        public static ProcessResult mappingOnly(String profileId, String message) {
            return ProcessResult.builder()
                    .action(Action.MAPPING_ONLY)
                    .profileId(profileId)
                    .message(message)
                    .profile(null)
                    .mappingCreated(true)
                    .build();
        }
    }
}