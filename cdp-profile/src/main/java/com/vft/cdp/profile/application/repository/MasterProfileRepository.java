package com.vft.cdp.profile.application.repository;

import com.vft.cdp.profile.domain.MasterProfile;

import java.util.List;
import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE REPOSITORY - APPLICATION LAYER PORT
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Application Layer - Repository Interface (Clean Architecture Port)
 *
 * Purpose: Defines contract for MasterProfile persistence
 * Implementation: EsMasterProfileRepositoryImpl (infrastructure layer)
 *
 * Following Clean Architecture:
 * - Application layer defines interface (port)
 * - Infrastructure provides implementation (adapter)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface MasterProfileRepository {

    /**
     * Save or update a master profile
     *
     * @param masterProfile the master profile to save
     * @return the saved master profile with updated metadata
     */
    MasterProfile save(MasterProfile masterProfile);

    /**
     * Find master profile by its master ID
     *
     * @param masterId the master profile ID
     * @return Optional containing the master profile if found
     */
    Optional<MasterProfile> findById(String masterId);

    /**
     * Find master profile that contains a specific merged profile ID
     *
     * Used to check if a profile has already been merged into a master profile
     *
     * @param profileId the profile ID to search for in merged profiles
     * @return Optional containing the master profile if found
     */
    Optional<MasterProfile> findByMergedProfileId(String profileId);

    /**
     * Find all master profiles for a tenant
     *
     * @param tenantId the tenant ID
     * @return list of master profiles
     */
    List<MasterProfile> findByTenantId(String tenantId);

    /**
     * Find all master profiles for a tenant and app
     *
     * @param tenantId the tenant ID
     * @param appId the app ID
     * @return list of master profiles
     */
    List<MasterProfile> findByTenantIdAndAppId(String tenantId, String appId);

    /**
     * Delete a master profile
     *
     * @param masterId the master profile ID to delete
     */
    void delete(String masterId);

    /**
     * Check if a master profile exists
     *
     * @param masterId the master profile ID
     * @return true if exists, false otherwise
     */
    boolean exists(String masterId);
}