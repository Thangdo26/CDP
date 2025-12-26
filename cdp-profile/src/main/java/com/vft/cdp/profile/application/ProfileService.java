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
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

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

    public ProfileDTO createProfile(CreateProfileCommand command) {
        log.info("📝 Creating profile: tenant={}, app={}, user={}",
                command.getTenantId(), command.getAppId(), command.getUserId());

        Optional<ProfileModel> existingOpt = profileRepository.find(
                command.getTenantId(),
                command.getAppId(),
                command.getUserId()
        );

        ProfileModel savedModel;

        if (existingOpt.isPresent()) {
            log.info("  ℹ️  Profile exists, merging...");

            Profile existing = convertToDomain(existingOpt.get());

            existing.update(
                    mapCommandTraitsToDomain(command.getTraits()),
                    mapCommandPlatformsToDomain(command.getPlatforms()),
                    mapCommandCampaignToDomain(command.getCampaign())
            );

            savedModel = profileRepository.save(existing);

        } else {
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

        cacheService.put(
                savedModel.getTenantId(),
                savedModel.getAppId(),
                savedModel.getUserId(),
                savedModel
        );

        ProfileDTO dto = ProfileDTOMapper.toDTO(savedModel);

        log.info("✅ Profile created/updated successfully");

        return dto;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // GET PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public Optional<ProfileDTO> getProfile(String tenantId, String appId, String userId) {

        Optional<ProfileModel> cached = cacheService.get(tenantId, appId, userId);
        if (cached.isPresent()) {
            log.debug("✅ Cache HIT: {}|{}|{}", tenantId, appId, userId);
            return Optional.of(ProfileDTOMapper.toDTO(cached.get()));
        }

        log.debug("❌ Cache MISS, loading from repository");
        Optional<ProfileModel> model = profileRepository.find(tenantId, appId, userId);

        model.ifPresent(m -> cacheService.put(tenantId, appId, userId, m));

        return model.map(ProfileDTOMapper::toDTO);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UPDATE PROFILE
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public ProfileDTO updateProfile(UpdateProfileCommand command) {
        log.info("🔄 Updating profile: {}|{}|{}",
                command.getTenantId(), command.getAppId(), command.getUserId());

        ProfileModel existing = profileRepository
                .find(command.getTenantId(), command.getAppId(), command.getUserId())
                .orElseThrow(() -> new ProfileNotFoundException(
                        command.getTenantId(),
                        command.getAppId(),
                        command.getUserId()
                ));

        Profile profile = convertToDomain(existing);

        profile.update(
                mapCommandTraitsToDomain(command.getTraits()),
                mapCommandPlatformsToDomain(command.getPlatforms()),
                mapCommandCampaignToDomain(command.getCampaign())
        );

        if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
            Map<String, Object> merged = new HashMap<>();
            if (profile.getMetadata() != null) {
                merged.putAll(profile.getMetadata());
            }
            merged.putAll(command.getMetadata());
            profile.setMetadata(merged);
        }

        ProfileModel saved = profileRepository.save(profile);

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

    public void deleteProfile(String tenantId, String appId, String userId) {
        log.info("🗑️  Deleting profile: {}|{}|{}", tenantId, appId, userId);

        ProfileModel existing = profileRepository
                .find(tenantId, appId, userId)
                .orElseThrow(() -> new ProfileNotFoundException(tenantId, appId, userId));

        Profile profile = convertToDomain(existing);

        profile.delete();

        profileRepository.save(profile);

        cacheService.evict(tenantId, appId, userId);

        log.info("✅ Profile deleted successfully");
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SEARCH
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    public Page<ProfileDTO> searchProfiles(SearchProfileRequest request) {
        log.debug("🔍 Searching profiles: tenant={}, criteria={}",
                request.getTenantId(), request);

        // Build pagination
        Sort sort = buildSort(request.getSortBy(), request.getSortOrder());
        PageRequest pageRequest = PageRequest.of(
                request.getPage(),
                request.getPageSize(),
                sort
        );

        // For now, simple implementation - just return all active profiles for the tenant
        // TODO: Implement actual search with criteria
        Page<ProfileModel> profilePage = profileRepository.findActiveProfiles(
                request.getTenantId(),
                pageRequest
        );

        return profilePage.map(ProfileDTOMapper::toDTO);
    }

    private Sort buildSort(String sortBy, String sortOrder) {
        if (sortBy == null || sortBy.isBlank()) {
            sortBy = "updated_at";
        }

        Sort.Direction direction = "asc".equalsIgnoreCase(sortOrder)
                ? Sort.Direction.ASC
                : Sort.Direction.DESC;

        return Sort.by(direction, sortBy);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER: CONVERT MODEL → DOMAIN
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private Profile convertToDomain(ProfileModel model) {
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

        if (traits instanceof Profile.Traits) {
            return (Profile.Traits) traits;
        }

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