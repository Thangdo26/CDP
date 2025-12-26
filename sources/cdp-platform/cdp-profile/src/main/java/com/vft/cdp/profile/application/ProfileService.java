package com.vft.cdp.profile.application;

import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.command.UpdateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.exception.ProfileNotFoundException;
import com.vft.cdp.profile.application.mapper.ProfileDTOMapper;
import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.application.repository.ProfileRepository;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.cache.ProfileCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE SERVICE - PURE DDD
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * ✅ No dependency on cdp-common
 * ✅ Works with ProfileModel interface
 * ✅ Uses Domain Profile for business logic
 * ✅ Returns ProfileDTO for API
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileService {

    private final ProfileRepository profileRepository;
    private final ProfileCacheService cacheService;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // CREATE PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Create new profile from command
     *
     * FLOW:
     * 1. Check if exists → Merge
     * 2. If new → Create domain entity
     * 3. Save to repository
     * 4. Cache result
     * 5. Return DTO
     */
    public ProfileDTO createProfile(CreateProfileCommand command) {
        log.info("📝 Creating profile: tenant={}, app={}, user={}",
                command.getTenantId(), command.getAppId(), command.getUserId());

        // 1. Check if exists
        Optional<ProfileModel> existingOpt = profileRepository.find(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId()
        );

        ProfileModel savedModel;

        if (existingOpt.isPresent()) {
            // EXISTS → MERGE
            log.info("  ℹ️  Profile exists, merging...");

            // Convert to domain entity for business logic
            Profile existing = convertToDomain(existingOpt.get());

            // Merge data
            existing.update(
                    mapCommandTraitsToDomain(command.getTraits()),
                    mapCommandPlatformsToDomain(command.getPlatforms()),
                    mapCommandCampaignToDomain(command.getCampaign())
            );

            // Save
            savedModel = profileRepository.save(existing);

        } else {
            // NEW → CREATE
            log.info("  ✨ Creating new profile");

            Profile newProfile = Profile.create(
                    command.getTenantId(),
                    command.getAppId(),
                    command.getUserId(),
                    command.getType(),
                    mapCommandTraitsToDomain(command.getTraits()),
                    mapCommandPlatformsToDomain(command.getPlatforms()),
                    mapCommandCampaignToDomain(command.getCampaign()),
                    command.getMetadata() != null ? command.getMetadata() : new HashMap<>()
            );

            savedModel = profileRepository.save(newProfile);
        }

        // 2. Cache
        cacheService.put(
                savedModel.getTenantId(),
                savedModel.getAppId(),
                savedModel.getUserId(),
                savedModel
        );

        // 3. Convert to DTO
        ProfileDTO dto = ProfileDTOMapper.toDTO(savedModel);

        log.info("✅ Profile created/updated successfully");

        return dto;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GET PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Get profile by ID (with caching)
     */
    public Optional<ProfileDTO> getProfile(String tenantId, String appId, String userId) {

        // 1. Try cache
        Optional<ProfileModel> cached = cacheService.get(tenantId, appId, userId);
        if (cached.isPresent()) {
            log.debug("✅ Cache HIT: {}|{}|{}", tenantId, appId, userId);
            return Optional.of(ProfileDTOMapper.toDTO(cached.get()));
        }

        // 2. Load from repository
        log.debug("❌ Cache MISS, loading from repository");
        Optional<ProfileModel> model = profileRepository.find(tenantId, appId, userId);

        // 3. Cache if found
        model.ifPresent(m -> cacheService.put(tenantId, appId, userId, m));

        // 4. Convert to DTO
        return model.map(ProfileDTOMapper::toDTO);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UPDATE PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Update existing profile
     */
    public ProfileDTO updateProfile(UpdateProfileCommand command) {
        log.info("🔄 Updating profile: {}|{}|{}",
                command.getTenantId(), command.getAppId(), command.getUserId());

        // 1. Load existing
        ProfileModel existing = profileRepository
                .find(command.getTenantId(), command.getAppId(), command.getUserId())
                .orElseThrow(() -> new ProfileNotFoundException(
                        command.getTenantId(),
                        command.getAppId(),
                        command.getUserId()
                ));

        // 2. Convert to domain for business logic
        Profile profile = convertToDomain(existing);

        // 3. Update
        profile.update(
                mapCommandTraitsToDomain(command.getTraits()),
                mapCommandPlatformsToDomain(command.getPlatforms()),
                mapCommandCampaignToDomain(command.getCampaign())
        );

        // 4. Update metadata
        if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
            Map<String, Object> merged = new HashMap<>();
            if (profile.getMetadata() != null) {
                merged.putAll(profile.getMetadata());
            }
            merged.putAll(command.getMetadata());
            profile.setMetadata(merged);
        }

        // 5. Save
        ProfileModel saved = profileRepository.save(profile);

        // 6. Invalidate cache
        cacheService.evict(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId()
        );

        log.info("✅ Profile updated successfully");

        return ProfileDTOMapper.toDTO(saved);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // DELETE PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Delete profile (soft delete)
     */
    public void deleteProfile(String tenantId, String appId, String userId) {
        log.info("🗑️  Deleting profile: {}|{}|{}", tenantId, appId, userId);

        // 1. Load
        ProfileModel existing = profileRepository
                .find(tenantId, appId, userId)
                .orElseThrow(() -> new ProfileNotFoundException(tenantId, appId, userId));

        // 2. Convert to domain
        Profile profile = convertToDomain(existing);

        // 3. Soft delete (business logic)
        profile.delete();

        // 4. Save
        profileRepository.save(profile);

        // 5. Invalidate cache
        cacheService.evict(tenantId, appId, userId);

        log.info("✅ Profile deleted successfully");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SEARCH
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Search profiles
     *
     * Note: This delegates to infrastructure layer
     * We need to add this method to ProfileRepository interface
     */
    public Page<ProfileDTO> searchProfiles(SearchProfileRequest request) {
        // TODO: Add search method to ProfileRepository interface
        throw new UnsupportedOperationException("Search not implemented yet");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER: CONVERT MODEL → DOMAIN
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Convert ProfileModel interface to Domain Profile entity
     *
     * WHY: When we need business logic (update, delete, merge)
     */
    private Profile convertToDomain(ProfileModel model) {
        // If already a Profile entity, return it
        if (model instanceof Profile) {
            return (Profile) model;
        }

        // Otherwise, reconstruct from model data
        return Profile.builder()
                .tenantId(model.getTenantId())
                .appId(model.getAppId())
                .userId(model.getUserId())
                .type(model.getType())
                .status(com.vft.cdp.profile.domain.ProfileStatus.fromValue(model.getStatus()))
                .mergedToMasterId(model.getMergedToMasterId())
                .mergedAt(model.getMergedAt())
                .traits(convertTraitsToDomain(model.getTraits()))
                .platforms(convertPlatformsToDomain(model.getPlatforms()))
                .campaign(convertCampaignToDomain(model.getCampaign()))
                .metadata(model.getMetadata())
                .createdAt(model.getCreatedAt())
                .updatedAt(model.getUpdatedAt())
                .firstSeenAt(model.getFirstSeenAt())
                .lastSeenAt(model.getLastSeenAt())
                .version(model.getVersion())
                .build();
    }

    private Profile.Traits convertTraitsToDomain(ProfileModel.TraitsModel traits) {
        if (traits == null) return null;

        // If already domain Traits, return it
        if (traits instanceof Profile.Traits) {
            return (Profile.Traits) traits;
        }

        // Reconstruct
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

    private Profile.Platforms convertPlatformsToDomain(ProfileModel.PlatformsModel platforms) {
        if (platforms == null) return null;

        if (platforms instanceof Profile.Platforms) {
            return (Profile.Platforms) platforms;
        }

        return Profile.Platforms.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private Profile.Campaign convertCampaignToDomain(ProfileModel.CampaignModel campaign) {
        if (campaign == null) return null;

        if (campaign instanceof Profile.Campaign) {
            return (Profile.Campaign) campaign;
        }

        return Profile.Campaign.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER: COMMAND → DOMAIN
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Profile.Traits mapCommandTraitsToDomain(CreateProfileCommand.TraitsCommand cmd) {
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

    private Profile.Platforms mapCommandPlatformsToDomain(CreateProfileCommand.PlatformsCommand cmd) {
        if (cmd == null) return null;

        return Profile.Platforms.builder()
                .os(cmd.getOs())
                .device(cmd.getDevice())
                .browser(cmd.getBrowser())
                .appVersion(cmd.getAppVersion())
                .build();
    }

    private Profile.Campaign mapCommandCampaignToDomain(CreateProfileCommand.CampaignCommand cmd) {
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
}