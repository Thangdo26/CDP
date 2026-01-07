package com.vft.cdp.profile.application.repository;

import java.util.Optional;
import java.util.List;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MAPPING REPOSITORY INTERFACE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Manages mappings between (tenant_id, app_id, user_id) and profile_id
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface ProfileMappingRepository {

    /**
     * Find mapping by tenant_id, app_id, user_id
     *
     * @return Optional containing profile_id if mapping exists
     */
    Optional<String> findProfileId(String tenantId, String appId, String userId);

    /**
     * Check if mapping exists
     */
    boolean exists(String tenantId, String appId, String userId);

    /**
     * Create or update mapping
     *
     * @param tenantId Tenant ID
     * @param appId App ID
     * @param userId User ID
     * @param profileId Profile ID in profiles_thang_dev
     */
    void saveMapping(String tenantId, String appId, String userId, String profileId);

    /**
     * Delete mapping
     */
    void deleteMapping(String tenantId, String appId, String userId);

    /**
     * Find all mappings pointing to a specific profile
     *
     * @param profileId Profile ID
     * @return List of mapping IDs (tenant_id|app_id|user_id)
     */
    List<String> findMappingsByProfileId(String profileId);

    /**
     * Count mappings for a profile
     */
    long countMappingsByProfileId(String profileId);
}