package com.vft.cdp.profile.application;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.RawProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import com.vft.cdp.profile.domain.repository.ProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Profile Service - NEW SCHEMA
 * Handles profile storage, retrieval, and merge logic
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
    public Optional<EnrichedProfile> getProfile(String tenantId, String userId) {
        return profileRepository.find(tenantId, userId);
    }

    /**
     * Save or merge enriched profile
     * - If profile exists: merge new data with existing data (preserve non-empty existing fields)
     * - If profile doesn't exist: create new
     */
    public void saveEnrichedProfile(EnrichedProfile enrichedProfile) {
        String tenantId = enrichedProfile.getTenantId();
        String userId = enrichedProfile.getUserId();  // Changed from profileId

        // 1. Check if profile exists
        Optional<EnrichedProfile> existingOpt = profileRepository.find(tenantId, userId);

        EnrichedProfile profileToSave;

        if (existingOpt.isPresent()) {
            // Profile exists → Merge
            EnrichedProfile existing = existingOpt.get();
            profileToSave = mergeProfiles(existing, enrichedProfile);

            log.info("🔄 MERGE - Updated profile tenant={} userId={}", tenantId, userId);
        } else {
            // Profile doesn't exist → Create new
            profileToSave = enrichedProfile;

            log.info("✨ NEW - Created profile tenant={} userId={}", tenantId, userId);
        }

        // 2. Save to ES
        profileRepository.save(profileToSave);

        log.info("💾 Saved profile tenant={} userId={}", tenantId, userId);
    }

    /**
     * Merge new profile data into existing profile
     * Strategy: Keep existing non-null values, only update with new non-null values
     */
    private EnrichedProfile mergeProfiles(EnrichedProfile existing, EnrichedProfile incoming) {
        return EnrichedProfile.builder()
                // Identity fields - never change
                .userId(existing.getUserId())
                .tenantId(existing.getTenantId())
                .appId(incoming.getAppId() != null ? incoming.getAppId() : existing.getAppId())

                // Profile data - update if incoming has value
                .type(incoming.getType() != null ? incoming.getType() : existing.getType())
                .traits(mergeTraits(existing.getTraits(), incoming.getTraits()))
                .platforms(incoming.getPlatforms() != null ? incoming.getPlatforms() : existing.getPlatforms())
                .campaign(incoming.getCampaign() != null ? incoming.getCampaign() : existing.getCampaign())
                .metadata(mergeMetadata(existing.getMetadata(), incoming.getMetadata()))

                // Enrichment metadata - use incoming
                .partitionKey(incoming.getPartitionKey())
                .enrichedAt(incoming.getEnrichedAt())
                .enrichedId(incoming.getEnrichedId())

                // Tracking timestamps - keep earliest created/first_seen, update latest updated/last_seen
                .createdAt(existing.getCreatedAt() != null ? existing.getCreatedAt() : incoming.getCreatedAt())
                .updatedAt(incoming.getUpdatedAt() != null ? incoming.getUpdatedAt() : existing.getUpdatedAt())
                .firstSeenAt(existing.getFirstSeenAt() != null ? existing.getFirstSeenAt() : incoming.getFirstSeenAt())
                .lastSeenAt(incoming.getLastSeenAt() != null ? incoming.getLastSeenAt() : existing.getLastSeenAt())

                // Version - increment
                .version(calculateVersion(existing.getVersion()))

                .build();
    }

    /**
     * Merge traits: merge field by field
     * Strategy: Incoming non-null values override existing values
     */
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

    /**
     * Merge metadata: merge both maps, incoming values override existing
     */
    private Map<String, Object> mergeMetadata(Map<String, Object> existing, Map<String, Object> incoming) {
        if (existing == null && incoming == null) {
            return null;
        }

        Map<String, Object> merged = new HashMap<>();

        if (existing != null) {
            merged.putAll(existing);
        }

        if (incoming != null) {
            merged.putAll(incoming);
        }

        return merged;
    }

    /**
     * Calculate version: increment from existing
     */
    private Integer calculateVersion(Integer existingVersion) {
        int currentVersion = existingVersion != null ? existingVersion : 0;
        return currentVersion + 1;
    }
}