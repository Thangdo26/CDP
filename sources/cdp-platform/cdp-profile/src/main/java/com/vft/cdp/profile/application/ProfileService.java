package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import com.vft.cdp.profile.api.request.UpdateProfileRequest;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.*;

/**
 * Profile Service - NEW SCHEMA
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileService {

    private final ProfileRepository profileRepository;

    /**
     * Search profiles by criteria
     */
    public Page<EnrichedProfile> searchProfiles(SearchProfileRequest request) {
        return profileRepository.search(request);
    }

    /**
     * Get profile by tenant and user ID
     */
    public Optional<EnrichedProfile> getProfile(String tenantId, String appId, String userId) {
        return profileRepository.find(tenantId, appId, userId);
    }

    /**
     * Save or merge enriched profile
     * - If exists: MERGE (keep existing non-null values)
     * - If new: CREATE
     */
    public void saveEnrichedProfile(EnrichedProfile enrichedProfile) {
        String tenantId = enrichedProfile.getTenantId();
        String userId = enrichedProfile.getUserId();
        String appId=enrichedProfile.getAppId();

        log.info("💾 Saving profile: tenant={}, userId={}", tenantId, userId);

        // 1. Check if exists
        Optional<EnrichedProfile> existingOpt = profileRepository.find(tenantId,appId, userId);

        EnrichedProfile profileToSave;

        if (existingOpt.isPresent()) {
            // EXISTS → MERGE
            EnrichedProfile existing = existingOpt.get();
            profileToSave = mergeProfiles(existing, enrichedProfile);

            log.info("🔄 MERGE - Updated existing profile");
        } else {
            // NEW → CREATE
            profileToSave = enrichedProfile;

            log.info("✨ NEW - Created new profile");
        }

        // 2. Save to ES
        profileRepository.save(profileToSave);
    }

    /**
     * Merge logic: Keep existing non-null, update with incoming non-null
     */
    private EnrichedProfile mergeProfiles(EnrichedProfile existing, EnrichedProfile incoming) {
        return EnrichedProfile.builder()
                // Identity - never change
                .appId(existing.getAppId())
                .tenantId(existing.getTenantId())
                .userId(incoming.getUserId() != null ? incoming.getUserId() : existing.getUserId())

                // Data - update if incoming has value
                .type(incoming.getType() != null ? incoming.getType() : existing.getType())
                .traits(mergeTraits(existing.getTraits(), incoming.getTraits()))
                .platforms(incoming.getPlatforms() != null ? incoming.getPlatforms() : existing.getPlatforms())
                .campaign(incoming.getCampaign() != null ? incoming.getCampaign() : existing.getCampaign())
                .metadata(mergeMetadata(existing.getMetadata(), incoming.getMetadata()))

                // System metadata
                .partitionKey(incoming.getPartitionKey())
                .enrichedAt(incoming.getEnrichedAt())
                .enrichedId(incoming.getEnrichedId())

                // Timestamps - keep earliest created/first_seen, latest updated/last_seen
                .createdAt(existing.getCreatedAt())  // Keep original
                .updatedAt(incoming.getUpdatedAt())  // Update to latest
                .firstSeenAt(existing.getFirstSeenAt())  // Keep earliest
                .lastSeenAt(incoming.getLastSeenAt())  // Update to latest

                // Version - increment
                .version(calculateVersion(existing.getVersion()))

                .build();
    }

    private RawProfile.Traits mergeTraits(RawProfile.Traits existing, RawProfile.Traits incoming) {
        if (existing == null && incoming == null) {
            return null;
        }
        if (existing == null) {
            return incoming;
        }
        if (incoming == null) {
            return existing;
        }

        return RawProfile.Traits.builder()
                .fullName(incoming.getFullName() != null ? incoming.getFullName() : existing.getFullName())
                .firstName(incoming.getFirstName() != null ? incoming.getFirstName() : existing.getFirstName())
                .lastName(incoming.getLastName() != null ? incoming.getLastName() : existing.getLastName())
                .idcard(incoming.getIdcard() != null ? incoming.getIdcard() : existing.getIdcard())
                .oldIdcard(incoming.getOldIdcard() != null ? incoming.getOldIdcard() : existing.getOldIdcard())
                .phone(incoming.getPhone() != null ? incoming.getPhone() : existing.getPhone())
                .email(incoming.getEmail() != null ? incoming.getEmail() : existing.getEmail())
                .gender(incoming.getGender() != null ? incoming.getGender() : existing.getGender())
                .dob(incoming.getDob() != null ? incoming.getDob() : existing.getDob())
                .address(incoming.getAddress() != null ? incoming.getAddress() : existing.getAddress())
                .religion(incoming.getReligion() != null ? incoming.getReligion() : existing.getReligion())
                .build();
    }

    private Map<String, Object> mergeMetadata(Map<String, Object> existing, Map<String, Object> incoming) {
        if (existing == null && incoming == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> merged = new HashMap<>();

        if (existing != null) {
            merged.putAll(existing);
        }

        if (incoming != null) {
            merged.putAll(incoming);  // Incoming values override
        }

        return merged;
    }

    // ========== NEW: UPDATE PROFILE ==========

    /*
     * Update existing profile with new data
     *
     * STRATEGY:
     * - Load existing profile
     * - Update only non-null fields from request
     * - Keep existing values for null fields
     * - Update timestamps
     * - Save back to ES
     *
     * @param tenantId Tenant ID
     * @param appId App ID
     * @param userId User ID
     * @param request Update request with fields to update
     * @return Updated profile
     * @throws ProfileNotFoundException if profile doesn't exist
     */
    public EnrichedProfile updateProfile(
            String tenantId,
            String appId,
            String userId,
            UpdateProfileRequest request) {

        log.info("🔄 Updating profile: tenant={}, app={}, user={}",
                tenantId, appId, userId);

        // 1. Load existing profile
        EnrichedProfile existing = profileRepository.find(tenantId, appId, userId)
                .orElseThrow(() -> {
                    log.warn("❌ Profile not found: {}|{}|{}", tenantId, appId, userId);
                    return new ResponseStatusException(
                            HttpStatus.NOT_FOUND,
                            String.format("Profile not found: %s|%s|%s", tenantId, appId, userId)
                    );
                });

        // 2. Build updated profile
        EnrichedProfile updated = buildUpdatedProfile(existing, request);

        // 3. Save to ES
        EnrichedProfile saved = profileRepository.save(updated);

        return saved;
    }

    /**
     * Build updated profile by merging existing + request data
     */
    private EnrichedProfile buildUpdatedProfile(
            EnrichedProfile existing,
            UpdateProfileRequest request) {

        // Merge từng phần
        RawProfile.Traits mergedTraits =
                mergeTraitsForUpdate(existing.getTraits(), request.getTraits());

        RawProfile.Platforms mergedPlatforms =
                mergePlatformsForUpdate(existing.getPlatforms(), request.getPlatforms());

        RawProfile.Campaign mergedCampaign =
                mergeCampaignForUpdate(existing.getCampaign(), request.getCampaign());

        Map<String, Object> mergedMetadata =
                mergeMetadataForUpdate(existing.getMetadata(), request.getMetadata());

        boolean isChanged = isProfileChanged(
                existing,
                request,
                mergedTraits,
                mergedPlatforms,
                mergedCampaign,
                mergedMetadata
        );

        return EnrichedProfile.builder()
                // Identity
                .tenantId(existing.getTenantId())
                .appId(existing.getAppId())
                .userId(existing.getUserId())

                // Type
                .type(request.getType() != null ? request.getType() : existing.getType())

                // Merged fields
                .traits(mergedTraits)
                .platforms(mergedPlatforms)
                .campaign(mergedCampaign)
                .metadata(mergedMetadata)

                // System metadata
                .partitionKey(existing.getPartitionKey())
                .enrichedAt(existing.getEnrichedAt())
                .enrichedId(existing.getEnrichedId())

                // Timestamps
                .createdAt(existing.getCreatedAt())
                .updatedAt(isChanged ? Instant.now() : existing.getUpdatedAt())
                .firstSeenAt(existing.getFirstSeenAt())
                .lastSeenAt(isChanged ? Instant.now() : existing.getLastSeenAt())

                // ✅ Version logic
                .version(isChanged
                        ? calculateVersion(existing.getVersion())
                        : existing.getVersion())

                .build();
    }

    //Hàm check xem có trường nào được update không
    private boolean isProfileChanged(
            EnrichedProfile existing,
            UpdateProfileRequest request,
            RawProfile.Traits mergedTraits,
            RawProfile.Platforms mergedPlatforms,
            RawProfile.Campaign mergedCampaign,
            Map<String, Object> mergedMetadata) {

        return isTypeChanged(existing, request)
                || !Objects.equals(existing.getTraits(), mergedTraits)
                || !Objects.equals(existing.getPlatforms(), mergedPlatforms)
                || !Objects.equals(existing.getCampaign(), mergedCampaign)
                || !Objects.equals(existing.getMetadata(), mergedMetadata);
    }

    private boolean isTypeChanged(
            EnrichedProfile existing,
            UpdateProfileRequest request) {

        return request.getType() != null
                && !Objects.equals(existing.getType(), request.getType());
    }

    private RawProfile.Traits mergeTraitsForUpdate(
            RawProfile.Traits existing,
            UpdateProfileRequest.TraitsUpdate update) {

        if (existing == null) {
            existing = new RawProfile.Traits();
        }

        if (update == null) {
            return existing;
        }

        return RawProfile.Traits.builder()
                .fullName(update.getFullName() != null ? update.getFullName() : existing.getFullName())
                .firstName(update.getFirstName() != null ? update.getFirstName() : existing.getFirstName())
                .lastName(update.getLastName() != null ? update.getLastName() : existing.getLastName())
                .idcard(update.getIdcard() != null ? update.getIdcard() : existing.getIdcard())
                .oldIdcard(update.getOldIdcard() != null ? update.getOldIdcard() : existing.getOldIdcard())
                .phone(update.getPhone() != null ? update.getPhone() : existing.getPhone())
                .email(update.getEmail() != null ? update.getEmail() : existing.getEmail())
                .gender(update.getGender() != null ? update.getGender() : existing.getGender())
                .dob(update.getDob() != null ? update.getDob() : existing.getDob())
                .address(update.getAddress() != null ? update.getAddress() : existing.getAddress())
                .religion(update.getReligion() != null ? update.getReligion() : existing.getReligion())
                .build();
    }

    private Integer calculateVersion(Integer existingVersion) {
        int currentVersion = existingVersion != null ? existingVersion : 0;
        return currentVersion + 1;
    }

    private RawProfile.Platforms mergePlatformsForUpdate(
            RawProfile.Platforms existing,
            UpdateProfileRequest.PlatformsUpdate update) {

        if (update == null) {
            return existing;
        }

        if (existing == null) {
            existing = new RawProfile.Platforms();
        }

        return RawProfile.Platforms.builder()
                .os(update.getOs() != null ? update.getOs() : existing.getOs())
                .device(update.getDevice() != null ? update.getDevice() : existing.getDevice())
                .browser(update.getBrowser() != null ? update.getBrowser() : existing.getBrowser())
                .appVersion(update.getAppVersion() != null ? update.getAppVersion() : existing.getAppVersion())
                .build();
    }

    /**
     * Merge campaign for update
     */
    private RawProfile.Campaign mergeCampaignForUpdate(
            RawProfile.Campaign existing,
            UpdateProfileRequest.CampaignUpdate update) {

        if (update == null) {
            return existing;
        }

        if (existing == null) {
            existing = new RawProfile.Campaign();
        }

        return RawProfile.Campaign.builder()
                .utmSource(update.getUtmSource() != null ? update.getUtmSource() : existing.getUtmSource())
                .utmCampaign(update.getUtmCampaign() != null ? update.getUtmCampaign() : existing.getUtmCampaign())
                .utmMedium(update.getUtmMedium() != null ? update.getUtmMedium() : existing.getUtmMedium())
                .utmContent(update.getUtmContent() != null ? update.getUtmContent() : existing.getUtmContent())
                .utmTerm(update.getUtmTerm() != null ? update.getUtmTerm() : existing.getUtmTerm())
                .utmCustom(update.getUtmCustom() != null ? update.getUtmCustom() : existing.getUtmCustom())
                .build();
    }

    /**
     * Merge metadata for update
     */
    private Map<String, Object> mergeMetadataForUpdate(
            Map<String, Object> existing,
            Map<String, Object> update) {

        Map<String, Object> result = new HashMap<>();

        if (existing != null) {
            result.putAll(existing);
        }

        if (update != null) {
            result.putAll(update);  // Override with new values
        }

        return result;
    }

// ========== DELETE PROFILE ==========
    /*
     * Delete profile from Elasticsearch
     *
     * STRATEGY:
     * - Soft delete: Update status to "DELETED"
     * - Hard delete: Remove from ES completely
     *
     * Current implementation: HARD DELETE
     *
     * @param tenantId Tenant ID
     * @param appId App ID
     * @param userId User ID
     * @return Deleted profile ID
     * @throws ProfileNotFoundException if profile doesn't exist
     */
    public String deleteProfile(String tenantId, String appId, String userId) {

        // 1. Delete from repository
        profileRepository.delete(tenantId, appId, userId);

        String profileId = tenantId + "|" + appId + "|" + userId;

        log.info("✅ Profile deleted successfully: {}", profileId);

        return profileId;
    }

}