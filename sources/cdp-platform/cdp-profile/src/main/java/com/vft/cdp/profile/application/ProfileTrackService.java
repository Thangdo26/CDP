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
            log.info("Incoming data is newer, updating profile");
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

        log.info("🔀 Merge Service: idcard={}", idcard);

        if (idcard == null || idcard.isBlank()) {
            log.info("🆕 No idcard, creating new profile");
            return createNewProfile(command);
        }

        // FIXED: Search by idcard within tenant
        List<ProfileModel> existingProfiles = profileRepository.findByIdcard(
                command.getTenantId(),
                idcard
        );

        if (!existingProfiles.isEmpty()) {
            ProfileModel existingProfile = existingProfiles.get(0);
            String profileId = extractProfileId(existingProfile);
            Instant existingUpdatedAt = existingProfile.getUpdatedAt();

            log.info("🔍 Found existing profile with idcard: profileId={}", profileId);

            if (shouldUpdate(incomingUpdatedAt, existingUpdatedAt)) {
                log.info("Updating existing profile and creating mapping");
                ProcessResult result = updateProfile(existingProfile, command, incomingUpdatedAt);

                // Create mapping
                mappingRepository.saveMapping(
                        command.getTenantId(),
                        command.getAppId(),
                        command.getUserId(),
                        profileId
                );

                Profile profile = convertToDomain(existingProfile);
                profile.addUser(command.getAppId(), command.getUserId());
                profileRepository.save(profile);
                result.setMappingCreated(true);

                return result;
            } else {
                log.info("⏭️ Just creating mapping, profile not updated");
                mappingRepository.saveMapping(
                        command.getTenantId(),
                        command.getAppId(),
                        command.getUserId(),
                        profileId
                );

                Profile profile = convertToDomain(existingProfile);
                profile.addUser(command.getAppId(), command.getUserId());
                profileRepository.save(profile);

                return ProcessResult.mappingOnly(
                        profileId,
                        "Mapping created, profile not updated (older data)"
                );
            }
        }

        log.info("🆕 No profile with idcard, creating new");
        return createNewProfile(command);
    }

    /**
     * FIXED: Create new profile with correct profileId
     */
    private ProcessResult createNewProfile(CreateProfileCommand command) {
        String idcard = command.getTraits() != null ? command.getTraits().getIdcard() : null;

        // Generate profileId FIRST
        String profileId = generateProfileId(idcard);

        log.info("🆕 Creating new profile: profileId={}", profileId);

        Instant now = Instant.now();

        // FIXED: Use profileId as the document ID (stored in userId field)
        Profile newProfile = Profile.builder()
                .tenantId(command.getTenantId())
                .appId(command.getAppId())
                .userId(profileId)
                .type(command.getType())
                .status(ProfileStatus.ACTIVE)
                .users(new ArrayList<>())
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

        newProfile.addUser(command.getAppId(), command.getUserId());
        // Save to ES
        ProfileModel saved = profileRepository.save(newProfile);

        // Create mapping with correct profileId
        mappingRepository.saveMapping(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                profileId
        );

        log.info("🔗 Mapping created: {}|{}|{} -> {}",
                command.getTenantId(), command.getAppId(), command.getUserId(), profileId);

        // FIXED: Cache AFTER everything is saved
        cacheService.put(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                saved
        );

        log.info("Profile created successfully: profileId={}", profileId);

        return ProcessResult.created(profileId, ProfileDTOMapper.toDTO(saved));
    }

    /**
     * FIXED: Update profile and cache AFTER
     */
    private ProcessResult updateProfile(
            ProfileModel existingModel,
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        String profileId = extractProfileId(existingModel);

        Profile profile = convertToDomain(existingModel);

        // Step 1: Update data (this does NOT touch updatedAt)
        profile.update(
                mapTraits(command.getTraits()),
                mapPlatforms(command.getPlatforms()),
                mapCampaign(command.getCampaign())
        );

        // Step 2: Update metadata
        if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
            profile.updateMetadata(command.getMetadata());
        }

        // Step 3: Set updatedAt from incoming metadata (AFTER update())
        if (incomingUpdatedAt != null) {
            profile.setUpdatedAt(incomingUpdatedAt);
            log.debug("⏰ Set updated_at from metadata: {}", incomingUpdatedAt);
        } else {
            profile.setUpdatedAt(Instant.now());
            log.debug("⏰ Set updated_at to now (no metadata timestamp)");
        }

        // Step 4: Save to ES
        ProfileModel saved = profileRepository.save(profile);

        // ADD THIS: Invalidate cache for ALL users linked to this profile
        invalidateCacheForProfile(profileId, command.getTenantId(), command.getAppId(), command.getUserId());


        // Step 5: Cache AFTER save
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
                }
            }
        }
    }

    /**
     * FIXED: Proper profileId generation
     */
    private String generateProfileId(String idcard) {
        if (idcard != null && !idcard.isBlank()) {
            return "idcard:" + idcard;
        }
        return "uuid:" + UUID.randomUUID().toString();
    }

    /**
     * FIXED: Extract profileId from ProfileModel
     */
    private String extractProfileId(ProfileModel model) {
        // ProfileId is stored in userId field (ES document _id)
        String userId = model.getUserId();

        if (userId != null && (userId.startsWith("idcard:") || userId.startsWith("uuid:"))) {
            return userId;
        }

        // Fallback: try to build from idcard
        if (model.getTraits() != null && model.getTraits().getIdcard() != null) {
            return "idcard:" + model.getTraits().getIdcard();
        }

        // Last resort: use legacy format
        return model.getTenantId() + "|" + model.getAppId() + "|" + userId;
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