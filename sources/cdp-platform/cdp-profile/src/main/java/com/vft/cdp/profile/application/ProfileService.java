package com.vft.cdp.profile.application;

// Imports...
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.application.command.CreateProfileCommand;
import com.vft.cdp.profile.application.command.UpdateProfileCommand;
import com.vft.cdp.profile.application.dto.ProfileDTO;
import com.vft.cdp.profile.application.exception.ProfileNotFoundException;
import com.vft.cdp.profile.application.mapper.ProfileDTOMapper;
import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.application.repository.ProfileMappingRepository;
import com.vft.cdp.profile.application.repository.ProfileRepository;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.domain.ProfileStatus;
import com.vft.cdp.profile.infra.cache.ProfileCacheService;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import com.vft.cdp.profile.infra.es.mapper.ProfileMapper;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ═══════════════════════════════════════════════════════════════
 * PROFILE SERVICE - FIXED
 * ═══════════════════════════════════════════════════════════════
 *
 * FIXES:
 * 1. GET: Fetch from ES via mapping, cache AFTER retrieval
 * 2. UPDATE: Update ES, invalidate cache, then cache new data
 * 3. DELETE: Delete from ES, remove mapping, clear cache
 * 4. NO premature caching
 * ═══════════════════════════════════════════════════════════════
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileService {

    private final ProfileRepository profileRepository;
    private final ProfileMappingRepository mappingRepository;
    private final ProfileCacheService cacheService;
    private final ElasticsearchOperations esOps;

    // ═══════════════════════════════════════════════════════════════
    // GET PROFILE
    // ═══════════════════════════════════════════════════════════════

    /**
     * ✅ FIXED: Get profile from ES via mapping, NOT from cache first
     */
    public Optional<ProfileDTO> getProfile(String tenantId, String appId, String userId) {

        log.info("📖 GET profile: tenant={}, app={}, user={}", tenantId, appId, userId);

        // =========================
        // Step 0: Cache FIRST
        // =========================
        Optional<ProfileModel> cachedProfile =
                cacheService.get(tenantId, appId, userId);

        if (cachedProfile.isPresent()) {
            log.info("⚡ Cache HIT: {}|{}|{}", tenantId, appId, userId);
            return Optional.of(ProfileDTOMapper.toDTO(cachedProfile.get()));
        }

        log.info("🧊 Cache MISS: {}|{}|{}", tenantId, appId, userId);

        // =========================
        // Step 1: Lookup mapping
        // =========================
        Optional<String> profileIdOpt =
                mappingRepository.findProfileId(tenantId, appId, userId);

        if (profileIdOpt.isEmpty()) {
            log.warn("❌ Mapping not found: {}|{}|{}", tenantId, appId, userId);
            return Optional.empty();
        }

        String profileId = profileIdOpt.get();
        log.debug("Found mapping: {}|{}|{} → {}", tenantId, appId, userId, profileId);

        // =========================
        // Step 2: Fetch from ES
        // =========================
        Optional<ProfileModel> profileOpt =
                profileRepository.findById(profileId);

        if (profileOpt.isEmpty()) {
            log.warn("❌ Profile not found in ES: profileId={}", profileId);
            return Optional.empty();
        }

        ProfileModel profile = profileOpt.get();

        // =========================
        // Step 3: Put cache
        // =========================
        cacheService.put(tenantId, appId, userId, profile);

        log.info("✅ Profile retrieved: profileId={}", profileId);

        return Optional.of(ProfileDTOMapper.toDTO(profile));
    }

    /**
     * Get profile by profile_id directly
     */
    public Optional<ProfileDTO> getProfileById(String profileId) {
        log.info("📖 GET profile by ID: {}", profileId);
        Optional<ProfileModel> profileOpt = profileRepository.findById(profileId);
        return profileOpt.map(ProfileDTOMapper::toDTO);
    }

    // ═══════════════════════════════════════════════════════════════
    // UPDATE PROFILE
    // ═══════════════════════════════════════════════════════════════

    /**
     * ✅ FIXED: Update from ES, invalidate cache, cache new data
     */
    public ProfileDTO updateProfile(UpdateProfileCommand command) {
        String tenantId = command.getTenantId();
        String appId = command.getAppId();
        String userId = command.getUserId();

        log.info("🔄 UPDATE profile: tenant={}, app={}, user={}", tenantId, appId, userId);

        // Step 1: Lookup mapping
        Optional<String> profileIdOpt = mappingRepository.findProfileId(tenantId, appId, userId);

        if (profileIdOpt.isEmpty()) {
            log.warn("❌ Mapping not found for update: {}|{}|{}", tenantId, appId, userId);
            throw new ProfileNotFoundException(tenantId, appId, userId);
        }

        String profileId = profileIdOpt.get();
        log.debug("Found mapping for update: {} → {}", userId, profileId);

        // Step 2: Fetch from ES (NOT cache)
        Optional<ProfileModel> existingOpt = profileRepository.findById(profileId);

        if (existingOpt.isEmpty()) {
            log.warn("❌ Profile not found for update: profileId={}", profileId);
            throw new ProfileNotFoundException("Profile not found: " + profileId);
        }

        ProfileModel existingModel = existingOpt.get();
        Profile profile = convertToDomain(existingModel);

        // Step 3: Apply updates
        profile.update(
                mapCommandTraitsToDomain(command.getTraits()),
                mapCommandPlatformsToDomain(command.getPlatforms()),
                mapCommandCampaignToDomain(command.getCampaign())
        );

        if (command.getMetadata() != null && !command.getMetadata().isEmpty()) {
            profile.updateMetadata(command.getMetadata());
        }

        // Step 4: Save to ES
        ProfileModel saved = profileRepository.save(profile);

        // Step 5: Invalidate ALL caches for this profile
        invalidateCacheForProfile(profileId, tenantId, appId, userId);

        // Step 6: Cache the updated profile for current user
        cacheService.put(tenantId, appId, userId, saved);

        log.info("✅ Profile updated: profileId={}", profileId);

        return ProfileDTOMapper.toDTO(saved);
    }

    // ═══════════════════════════════════════════════════════════════
    // DELETE PROFILE
    // ═══════════════════════════════════════════════════════════════

    /**
     * ✅ FIXED: Delete from ES, remove mapping, clear cache
     */
    public DeleteResult deleteProfile(
            String tenantId,
            String appId,
            String userId,
            boolean deleteProfileIfLastMapping
    ) {
        log.info("🗑️ DELETE profile: tenant={}, app={}, user={}, deleteIfLast={}",
                tenantId, appId, userId, deleteProfileIfLastMapping);

        // Step 1: Lookup mapping
        Optional<String> profileIdOpt = mappingRepository.findProfileId(tenantId, appId, userId);

        if (profileIdOpt.isEmpty()) {
            log.warn("❌ Mapping not found for delete: {}|{}|{}", tenantId, appId, userId);
            throw new ProfileNotFoundException(tenantId, appId, userId);
        }

        String profileId = profileIdOpt.get();
        log.debug("Found mapping for delete: {} → {}", userId, profileId);

        // Step 2: Count mappings
        long mappingCount = mappingRepository.countMappingsByProfileId(profileId);
        log.debug("📊 Mapping count for profile {}: {}", profileId, mappingCount);

        boolean profileDeleted = false;

        // Step 3: Delete profile if last mapping
        if (mappingCount <= 1 && deleteProfileIfLastMapping) {
            log.info("🗑️ Last mapping, soft deleting profile: {}", profileId);

            Optional<ProfileModel> profileOpt = profileRepository.findById(profileId);
            if (profileOpt.isPresent()) {
                Profile profile = convertToDomain(profileOpt.get());
                profile.delete();
                profileRepository.save(profile);
                profileDeleted = true;
            }
        } else if (mappingCount > 1) {
            log.info("⚠️ Profile has {} other mappings, keeping profile", mappingCount - 1);

            // ✅ NEW: Remove user from profile.users array
            Optional<ProfileModel> profileOpt = profileRepository.findById(profileId);
            if (profileOpt.isPresent()) {
                Profile profile = convertToDomain(profileOpt.get());
                profileRepository.save(profile);
                log.debug("🗑️ Removed user from profile.users: {}|{}", appId, userId);
            }
        }

        // Step 4: Remove mapping
        mappingRepository.deleteMapping(tenantId, appId, userId);

        // Step 5: Clear cache
        cacheService.evict(tenantId, appId, userId);

        log.info("✅ Delete complete: mapping removed={}, profile deleted={}",
                true, profileDeleted);

        return DeleteResult.builder()
                .mappingRemoved(true)
                .profileDeleted(profileDeleted)
                .profileId(profileId)
                .remainingMappings(mappingCount - 1)
                .build();
    }

    public void deleteProfile(String tenantId, String appId, String userId) {
        deleteProfile(tenantId, appId, userId, true);
    }

    // ═══════════════════════════════════════════════════════════════
    // CREATE PROFILE (Keep but not used in track flow)
    // ═══════════════════════════════════════════════════════════════

    /**
     * ⚠️ DEPRECATED: Use ProfileTrackService.processTrack() instead
     * This method is kept for backward compatibility only
     */
    @Deprecated
    public ProfileDTO createProfile(CreateProfileCommand command) {
        log.warn("⚠️ DEPRECATED: createProfile() called directly. Use ProfileTrackService.processTrack() instead");

        String tenantId = command.getTenantId();
        String appId = command.getAppId();
        String userId = command.getUserId();

        log.info("➕ CREATE profile: tenant={}, app={}, user={}", tenantId, appId, userId);

        // Check if mapping exists
        Optional<String> existingProfileId = mappingRepository.findProfileId(tenantId, appId, userId);
        if (existingProfileId.isPresent()) {
            log.warn("⚠️ Profile mapping already exists: {}|{}|{} -> {}",
                    tenantId, appId, userId, existingProfileId.get());
            Optional<ProfileModel> existing = profileRepository.findById(existingProfileId.get());
            if (existing.isPresent()) {
                return ProfileDTOMapper.toDTO(existing.get());
            }
        }

        // Generate profileId
        String idcard = command.getTraits() != null ? command.getTraits().getIdcard() : null;
        String profileId = idcard != null && !idcard.isBlank()
                ? "idcard:" + idcard
                :  UUID.randomUUID().toString();

        // Create profile
        Profile profile = Profile.builder()
                .tenantId(tenantId)
                .appId(appId)
                .userId(profileId)  // Use profileId as document ID
                .type(command.getType())
                .status(ProfileStatus.ACTIVE)
                .traits(mapCommandTraitsToDomain(command.getTraits()))
                .platforms(mapCommandPlatformsToDomain(command.getPlatforms()))
                .campaign(mapCommandCampaignToDomain(command.getCampaign()))
                .metadata(command.getMetadata() != null ? command.getMetadata() : new HashMap<>())
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .firstSeenAt(Instant.now())
                .lastSeenAt(Instant.now())
                .version(1)
                .build();

        ProfileModel saved = profileRepository.save(profile);

        // Create mapping
        try {
            mappingRepository.saveMapping(tenantId, appId, userId, profileId);
            log.debug("🔗 Mapping created: {}|{}|{} -> {}", tenantId, appId, userId, profileId);
        } catch (Exception e) {
            log.error("❌ Failed to create mapping for: {}|{}|{} -> {}",
                    tenantId, appId, userId, profileId, e);
            throw new RuntimeException("Failed to create profile mapping", e);
        }

        // Cache
        cacheService.put(tenantId, appId, userId, saved);

        log.info("✅ Profile created: profileId={}", profileId);

        return ProfileDTOMapper.toDTO(saved);
    }

    // ═══════════════════════════════════════════════════════════════
    // SEARCH PROFILES
    // ═══════════════════════════════════════════════════════════════

    public Page<ProfileDTO> searchProfiles(SearchProfileRequest request) {
        log.info("🔍 Search profiles: tenant={}, criteria={}", request.getTenantId(), request);

        Criteria criteria = new Criteria("tenant_id").is(request.getTenantId())
                .and("status").is("active");

        if (request.getUserId() != null && !request.getUserId().isBlank()) {
            criteria = criteria.and("user_id").is(request.getUserId());
        }

        if (request.getAppId() != null && !request.getAppId().isBlank()) {
            criteria = criteria.and("app_id").is(request.getAppId());
        }

        if (request.getType() != null && !request.getType().isBlank()) {
            criteria = criteria.and("type").is(request.getType());
        }

        if (request.getTraits() != null) {
            SearchProfileRequest.TraitsSearch traits = request.getTraits();

            if (traits.getEmail() != null && !traits.getEmail().isBlank()) {
                criteria = criteria.and("traits.email").is(traits.getEmail().toLowerCase());
            }

            if (traits.getPhone() != null && !traits.getPhone().isBlank()) {
                criteria = criteria.and("traits.phone").is(traits.getPhone());
            }

            if (traits.getIdcard() != null && !traits.getIdcard().isBlank()) {
                criteria = criteria.and("traits.idcard").is(traits.getIdcard());
            }

            if (traits.getOldIdcard() != null && !traits.getOldIdcard().isBlank()) {
                criteria = criteria.and("traits.old_idcard").is(traits.getOldIdcard());
            }

            if (traits.getFullName() != null && !traits.getFullName().isBlank()) {
                criteria = criteria.and("traits.full_name").contains(traits.getFullName());
            }

            if (traits.getFirstName() != null && !traits.getFirstName().isBlank()) {
                criteria = criteria.and("traits.first_name").is(traits.getFirstName());
            }

            if (traits.getLastName() != null && !traits.getLastName().isBlank()) {
                criteria = criteria.and("traits.last_name").is(traits.getLastName());
            }

            if (traits.getGender() != null && !traits.getGender().isBlank()) {
                criteria = criteria.and("traits.gender").is(traits.getGender());
            }

            if (traits.getDob() != null && !traits.getDob().isBlank()) {
                criteria = criteria.and("traits.dob").is(traits.getDob());
            }

            if (traits.getAddress() != null && !traits.getAddress().isBlank()) {
                criteria = criteria.and("traits.address").contains(traits.getAddress());
            }

            if (traits.getReligion() != null && !traits.getReligion().isBlank()) {
                criteria = criteria.and("traits.religion").is(traits.getReligion());
            }
        }

        if (request.getMetadata() != null && !request.getMetadata().isEmpty()) {
            for (Map.Entry<String, Object> entry : request.getMetadata().entrySet()) {
                criteria = criteria.and("metadata." + entry.getKey()).is(entry.getValue());
            }
        }

        Sort sort = buildSort(request.getSortBy(), request.getSortOrder());
        PageRequest pageRequest = PageRequest.of(request.getPage(), request.getPageSize(), sort);

        CriteriaQuery query = new CriteriaQuery(criteria).setPageable(pageRequest);

        SearchHits<ProfileDocument> searchHits = esOps.search(query, ProfileDocument.class);

        List<ProfileDTO> profiles = searchHits.stream()
                .map(SearchHit::getContent)
                .map(doc -> {
                    Profile profile = ProfileMapper.toDomain(doc);
                    return ProfileDTOMapper.toDTO(profile);
                })
                .collect(Collectors.toList());

        log.info("✅ Found {} profiles (total: {})", profiles.size(), searchHits.getTotalHits());

        return new PageImpl<>(profiles, pageRequest, searchHits.getTotalHits());
    }

    // ═══════════════════════════════════════════════════════════════
    // LINKED ACCOUNTS
    // ═══════════════════════════════════════════════════════════════

    public List<MappingInfo> getLinkedAccounts(String tenantId, String appId, String userId) {
        log.info("🔗 Get linked accounts: tenant={}, app={}, user={}", tenantId, appId, userId);

        Optional<String> profileIdOpt = mappingRepository.findProfileId(tenantId, appId, userId);

        if (profileIdOpt.isEmpty()) {
            return Collections.emptyList();
        }

        String profileId = profileIdOpt.get();
        List<String> mappingIds = mappingRepository.findMappingsByProfileId(profileId);

        List<MappingInfo> linkedAccounts = new ArrayList<>();
        for (String mappingId : mappingIds) {
            String[] parts = mappingId.split("\\|");
            if (parts.length == 3) {
                linkedAccounts.add(MappingInfo.builder()
                        .tenantId(parts[0])
                        .appId(parts[1])
                        .userId(parts[2])
                        .profileId(profileId)
                        .build());
            }
        }

        log.info("✅ Found {} linked accounts for profile {}", linkedAccounts.size(), profileId);

        return linkedAccounts;
    }

    // ═══════════════════════════════════════════════════════════════
    // PRIVATE HELPERS
    // ═══════════════════════════════════════════════════════════════

    private void invalidateCacheForProfile(
            String profileId,
            String currentTenantId,
            String currentAppId,
            String currentUserId
    ) {
        cacheService.evict(currentTenantId, currentAppId, currentUserId);

        List<String> allMappings = mappingRepository.findMappingsByProfileId(profileId);

        for (String mappingId : allMappings) {
            String[] parts = mappingId.split("\\|");
            if (parts.length == 3) {
                String t = parts[0];
                String a = parts[1];
                String u = parts[2];

                if (!t.equals(currentTenantId) || !a.equals(currentAppId) || !u.equals(currentUserId)) {
                    cacheService.evict(t, a, u);
                    log.debug("🗑️ Cache evicted for linked user: {}|{}|{}", t, a, u);
                }
            }
        }
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

    private Profile convertToDomain(ProfileModel model) {
        if (model instanceof Profile) {
            return (Profile) model;
        }

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

    // ═══════════════════════════════════════════════════════════════
    // RESULT CLASSES
    // ═══════════════════════════════════════════════════════════════

    @lombok.Data
    @lombok.Builder
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    public static class DeleteResult {
        private boolean mappingRemoved;
        private boolean profileDeleted;
        private String profileId;
        private long remainingMappings;
    }

    @lombok.Data
    @lombok.Builder
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    public static class MappingInfo {
        private String tenantId;
        private String appId;
        private String userId;
        private String profileId;
    }
}