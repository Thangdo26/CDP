package com.vft.cdp.profile.domain.repository;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.profile.api.request.SearchProfileRequest;
import org.springframework.data.domain.Page;

import java.util.Optional;

/**
 * Profile Repository Interface - Domain contract
 */
public interface ProfileRepository {

    /**
     * Find profile by composite key
     *
     * @param tenantId Tenant ID (VC, VAM, VHM)
     * @param appId App ID (VTMN, MOMO, VFAST_APP)
     * @param userId External user ID
     * @return Optional profile
     */
    Optional<EnrichedProfile> find(String tenantId, String appId, String userId);
    /**
     * Save or update profile
     */
    EnrichedProfile save(EnrichedProfile profile);

    /**
     * Search profiles by criteria
     */
    Page<EnrichedProfile> search(SearchProfileRequest request);

    void delete(String tenantId, String appId, String userId);
}