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
import com.vft.cdp.profile.utils.AutoIdUtil;
import com.vft.cdp.profile.utils.PhoneUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE TRACK SERVICE - UPDATED WITH MATCHING LOGIC
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * CHANGES:
 * 1. createNewProfile: Don't initialize users[] - let repository handle it
 * 2. handleMergeByMatching: Use existing profile_id for mapping when match found
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileTrackService {

    private final ProfileMappingRepository mappingRepository;
    private final ProfileRepository profileRepository;
    private final ProfileCacheService cacheService;
    private final ProfileMatchingService matchingService;

    public ProcessResult processTrack(CreateProfileCommand command) {
        String tenantId = command.getTenantId();
        String appId = command.getAppId();
        String userId = command.getUserId();

        log.info("🔥 Processing track: tenant={}, app={}, user={}",
                tenantId, appId, userId);

        Instant incomingUpdatedAt = extractUpdatedAt(command.getMetadata());

        // Step 1: Check profile_mapping
        Optional<String> existingProfileId = mappingRepository.findProfileId(tenantId, appId, userId);

        if (existingProfileId.isPresent()) {
            return handleExistingMapping(existingProfileId.get(), command, incomingUpdatedAt);
        }

        // Step 2: Merge Service - Find by multiple criteria
        return handleMergeByMatching(command, incomingUpdatedAt);
    }

    private ProcessResult handleExistingMapping(
            String profileId,
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        log.info("Found existing mapping → profile_id={}", profileId);

        Optional<ProfileModel> profileOpt = profileRepository.findById(profileId);

        if (profileOpt.isEmpty()) {
            log.warn("⚠️ Profile not found for mapping! Creating new...");
            return createNewProfile(command);
        }

        ProfileModel existingProfile = profileOpt.get();
        Instant existingUpdatedAt = existingProfile.getUpdatedAt();

        if (shouldUpdate(incomingUpdatedAt, existingUpdatedAt)) {
            log.info("Incoming data is newer, updating profile");
            return updateExistingProfile(existingProfile, command, incomingUpdatedAt);
        } else {
            log.info("Incoming data is older/same, skipping update");
            return ProcessResult.skipped(profileId, "Incoming updated_at <= existing updated_at");
        }
    }

    private ProcessResult handleMergeByMatching(
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        String tenantId = command.getTenantId();
        String appId = command.getAppId();
        String userId = command.getUserId();

        // Extract traits
        String idcard = command.getTraits() != null ? command.getTraits().getIdcard() : null;
        String phone = command.getTraits() != null ? command.getTraits().getPhone() : null;
        String email = command.getTraits() != null ? command.getTraits().getEmail() : null;
        String dob = command.getTraits() != null ? command.getTraits().getDob() : null;

        log.info("🔍 Merge Service: tenant={}, idcard={}, phone={}, email={}, dob={}",
                tenantId, idcard, phone, email, dob);

        // ═══════════════════════════════════════════════════════════════
        // Find matching profile using multiple strategies
        // ═══════════════════════════════════════════════════════════════
        ProfileMatchingService.MatchResult matchResult = matchingService.findMatchingProfile(
                tenantId, idcard, email, dob
        );

        if (!matchResult.isFound()) {
            log.info("🆕 No matching profile, creating new");
            return createNewProfile(command);
        }

        // ═══════════════════════════════════════════════════════════════
        // ✅ FIXED: Found matching profile → Use existing profile_id for mapping
        // ═══════════════════════════════════════════════════════════════
        ProfileModel existingProfile = matchResult.getProfile();
        String existingProfileId = extractProfileId(existingProfile);

        log.info("🔍 Found existing profile: id={}, strategy={}",
                existingProfileId, matchResult.getStrategy().getValue());

        // ✅ Create mapping using EXISTING profile_id
        mappingRepository.saveMapping(tenantId, appId, userId, existingProfileId);
        log.info("🔗 Mapping created: {}|{}|{} → {}", tenantId, appId, userId, existingProfileId);

        // ✅ Convert to domain
        Profile profile = convertToDomain(existingProfile);

        Instant existingUpdatedAt = existingProfile.getUpdatedAt();

        // Check if we need to update profile data
        if (shouldUpdate(incomingUpdatedAt, existingUpdatedAt)) {
            log.info("✏️ Updating profile with newer data");

            // Merge data
            mergeProfileData(profile, command);

            // Set timestamps
            if (incomingUpdatedAt != null) {
                profile.setUpdatedAt(incomingUpdatedAt);
            } else {
                profile.setUpdatedAt(Instant.now());
            }

            profile.setVersion((profile.getVersion() == null ? 0 : profile.getVersion()) + 1);
            profile.setLastSeenAt(Instant.now());

            // Save
            ProfileModel saved = profileRepository.save(profile);

            // Invalidate cache
            invalidateCacheForProfile(existingProfileId, tenantId, appId, userId);
            cacheService.put(tenantId, appId, userId, saved);

            log.info("✅ Profile merged and updated: profileId={}, strategy={}",
                    existingProfileId, matchResult.getStrategy().getValue());

            return ProcessResult.updated(existingProfileId, ProfileDTOMapper.toDTO(saved));
        } else {
            log.info("⏭️ Data is older/same, only mapping created");

            // Just save to trigger users[] rebuild
            ProfileModel saved = profileRepository.save(profile);

            cacheService.put(tenantId, appId, userId, saved);

            return ProcessResult.mappingOnly(
                    existingProfileId,
                    "Mapping created via " + matchResult.getStrategy().getValue() + " match"
            );
        }
    }

    /**
     * 🔥 NEW: Merge incoming data into existing profile
     * This keeps existing data and only updates with non-null incoming data
     */
    private void mergeProfileData(Profile existingProfile, CreateProfileCommand command) {
        // Merge traits
        Profile.Traits incomingTraits = mapTraits(command.getTraits());
        if (incomingTraits != null) {
            Profile.Traits existingTraits = existingProfile.getTraits();
            if (existingTraits == null) {
                existingProfile.setTraits(incomingTraits);
            } else {
                // Merge each field individually - keep existing if incoming is null
                if (incomingTraits.getFullName() != null) {
                    existingTraits.setFullName(incomingTraits.getFullName());
                }
                if (incomingTraits.getFirstName() != null) {
                    existingTraits.setFirstName(incomingTraits.getFirstName());
                }
                if (incomingTraits.getLastName() != null) {
                    existingTraits.setLastName(incomingTraits.getLastName());
                }
                if (incomingTraits.getIdcard() != null) {
                    existingTraits.setIdcard(incomingTraits.getIdcard());
                }
                if (incomingTraits.getOldIdcard() != null) {
                    existingTraits.setOldIdcard(incomingTraits.getOldIdcard());
                }
                if (incomingTraits.getPhone() != null && !incomingTraits.getPhone().isEmpty()) {
                    if (existingTraits.getPhone() == null) existingTraits.setPhone(new ArrayList<>());
                    List<String> merged = PhoneUtil.union(existingTraits.getPhone(), incomingTraits.getPhone());
                    existingTraits.setPhone(merged);
                }
                if (incomingTraits.getEmail() != null) {
                    existingTraits.setEmail(incomingTraits.getEmail());
                }
                if (incomingTraits.getGender() != null) {
                    existingTraits.setGender(incomingTraits.getGender());
                }
                if (incomingTraits.getDob() != null) {
                    existingTraits.setDob(incomingTraits.getDob());
                }
                if (incomingTraits.getAddress() != null) {
                    existingTraits.setAddress(incomingTraits.getAddress());
                }
                if (incomingTraits.getReligion() != null) {
                    existingTraits.setReligion(incomingTraits.getReligion());
                }
            }
        }

        // Merge platforms
        Profile.Platforms incomingPlatforms = mapPlatforms(command.getPlatforms());
        if (incomingPlatforms != null) {
            Profile.Platforms existingPlatforms = existingProfile.getPlatforms();
            if (existingPlatforms == null) {
                existingProfile.setPlatforms(incomingPlatforms);
            } else {
                if (incomingPlatforms.getOs() != null) {
                    existingPlatforms.setOs(incomingPlatforms.getOs());
                }
                if (incomingPlatforms.getDevice() != null) {
                    existingPlatforms.setDevice(incomingPlatforms.getDevice());
                }
                if (incomingPlatforms.getBrowser() != null) {
                    existingPlatforms.setBrowser(incomingPlatforms.getBrowser());
                }
                if (incomingPlatforms.getAppVersion() != null) {
                    existingPlatforms.setAppVersion(incomingPlatforms.getAppVersion());
                }
            }
        }

        // Merge campaign
        Profile.Campaign incomingCampaign = mapCampaign(command.getCampaign());
        if (incomingCampaign != null) {
            Profile.Campaign existingCampaign = existingProfile.getCampaign();
            if (existingCampaign == null) {
                existingProfile.setCampaign(incomingCampaign);
            } else {
                if (incomingCampaign.getUtmSource() != null) {
                    existingCampaign.setUtmSource(incomingCampaign.getUtmSource());
                }
                if (incomingCampaign.getUtmCampaign() != null) {
                    existingCampaign.setUtmCampaign(incomingCampaign.getUtmCampaign());
                }
                if (incomingCampaign.getUtmMedium() != null) {
                    existingCampaign.setUtmMedium(incomingCampaign.getUtmMedium());
                }
                if (incomingCampaign.getUtmContent() != null) {
                    existingCampaign.setUtmContent(incomingCampaign.getUtmContent());
                }
                if (incomingCampaign.getUtmTerm() != null) {
                    existingCampaign.setUtmTerm(incomingCampaign.getUtmTerm());
                }
                if (incomingCampaign.getUtmCustom() != null) {
                    existingCampaign.setUtmCustom(incomingCampaign.getUtmCustom());
                }
            }
        }

        // Merge metadata
        if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
            Map<String, Object> existingMetadata = existingProfile.getMetadata();
            if (existingMetadata == null) {
                existingProfile.setMetadata(new HashMap<>(command.getMetadata()));
            } else {
                existingMetadata.putAll(command.getMetadata());
            }
        }

        // Update type if provided
        if (command.getType() != null) {
            existingProfile.setType(command.getType());
        }
    }

    /**
     * ✅ FIXED: Initialize users[] with current user to avoid race condition
     * Previously: Let repository rebuild from mapping (but mapping not refreshed yet)
     * Now: Manually add current user, repository will sync with mappings on next update
     */
    private ProcessResult createNewProfile(CreateProfileCommand command) {
        // ✅ Generate UUID for profile ID
        String profileId = AutoIdUtil.genProfileId();

        Instant now = Instant.now();

        // ✅ FIXED: Initialize users[] with the current user
        List<Profile.UserIdentity> initialUsers = new ArrayList<>();
        initialUsers.add(Profile.UserIdentity.builder()
                .appId(command.getAppId())
                .userId(command.getUserId())
                .build());

        Profile newProfile = Profile.builder()
                .tenantId(command.getTenantId())
                .appId(command.getAppId())
                .userId(profileId)
                .type(command.getType())
                .status(ProfileStatus.ACTIVE)
                // ✅ FIXED: Initialize with current user to avoid race condition
                .users(initialUsers)
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

        // Create mapping
        mappingRepository.saveMapping(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                profileId
        );

        // Save profile (users[] already populated, repository will sync with mappings)
        ProfileModel saved = profileRepository.save(newProfile);

        // Cache
        cacheService.put(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId(),
                saved
        );

        log.info("✅ Master profile created: profileId={}, users[]={}",
                profileId, saved.getUsers() != null ? saved.getUsers().size() : 0);

        return ProcessResult.created(profileId, ProfileDTOMapper.toDTO(saved));
    }

    private ProcessResult updateExistingProfile(
            ProfileModel existingModel,
            CreateProfileCommand command,
            Instant incomingUpdatedAt
    ) {
        String profileId = extractProfileId(existingModel);

        Profile profile = convertToDomain(existingModel);

        // 🔥 FIX: Merge data instead of overwriting
        mergeProfileData(profile, command);

        // Set updatedAt
        if (incomingUpdatedAt != null) {
            profile.setUpdatedAt(incomingUpdatedAt);
        } else {
            profile.setUpdatedAt(Instant.now());
        }

        profile.setVersion((profile.getVersion() == null ? 0 : profile.getVersion()) + 1);
        profile.setLastSeenAt(Instant.now());

        // Save to ES
        ProfileModel saved = profileRepository.save(profile);

        // Invalidate cache for ALL users
        invalidateCacheForProfile(profileId, command.getTenantId(), command.getAppId(), command.getUserId());

        // Cache for current user
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

    private String generateProfileId(String idcard) {
        return AutoIdUtil.genProfileId();
    }

    private String extractProfileId(ProfileModel model) {
        return model.getUserId();
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
                .users(convertUsers(model.getUsers()))
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

    private List<Profile.UserIdentity> convertUsers(List<? extends ProfileModel.UserIdentityModel> users) {
        if (users == null) return new ArrayList<>();

        List<Profile.UserIdentity> result = new ArrayList<>();
        for (ProfileModel.UserIdentityModel u : users) {
            result.add(Profile.UserIdentity.builder()
                    .appId(u.getAppId())
                    .userId(u.getUserId())
                    .build());
        }
        return result;
    }

    private Profile.Traits mapTraits(CreateProfileCommand.TraitsCommand cmd) {
        if (cmd == null) return null;
        List<String> phones = new ArrayList<>();
        String p = PhoneUtil.normalize(cmd.getPhone());
        if (p != null) phones.add(p);

        return Profile.Traits.builder()
                .fullName(cmd.getFullName())
                .firstName(cmd.getFirstName())
                .lastName(cmd.getLastName())
                .idcard(cmd.getIdcard())
                .oldIdcard(cmd.getOldIdcard())
                .phone(phones)
                .email(cmd.getEmail() != null ? cmd.getEmail().trim().toLowerCase() : null)
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