package com.vft.cdp.profile.application.repository;

import com.vft.cdp.profile.application.model.ProfileModel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE REPOSITORY (APPLICATION INTERFACE)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * PATTERN: Repository Interface in Application Layer
 *
 * WHY HERE?
 * - Application layer defines WHAT it needs
 * - Infrastructure layer provides HOW (implementation)
 * - Domain layer doesn't depend on infrastructure
 * - Application doesn't depend on infrastructure
 *
 * DEPENDENCY RULE:
 * Infrastructure → Application (implements interface)
 * NOT: Application → Infrastructure
 *
 * IMPLEMENTATIONS:
 * - EsProfileRepository (Infrastructure) - Elasticsearch implementation
 * - MongoProfileRepository (Infrastructure) - MongoDB implementation
 * - InMemoryProfileRepository (Test) - In-memory for testing
 *
 * RETURNS:
 * - ProfileModel interface (NOT Domain entity, NOT ES document)
 * - Application works ONLY with ProfileModel
 * - Infrastructure decides which implementation to return
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface ProfileRepository {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BASIC CRUD
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Save or update profile
     *
     * @param profile Profile model to save
     * @return Saved profile model
     */
    ProfileModel save(ProfileModel profile);

    /**
     * Find profile by identity
     *
     * @param tenantId Tenant ID
     * @param appId Application ID
     * @param userId User ID
     * @return Profile if found
     */
    Optional<ProfileModel> find(String tenantId, String appId, String userId);

    /**
     * Delete profile by identity
     *
     * @param tenantId Tenant ID
     * @param appId Application ID
     * @param userId User ID
     */
    void delete(String tenantId, String appId, String userId);

    /**
     * Check if profile exists
     *
     * @param tenantId Tenant ID
     * @param appId Application ID
     * @param userId User ID
     * @return true if exists
     */
    boolean exists(String tenantId, String appId, String userId);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY BY STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find all profiles with specific status
     *
     * @param tenantId Tenant ID
     * @param status Profile status (active, merged, deleted)
     * @param pageable Pagination
     * @return Page of profiles
     */
    Page<ProfileModel> findByStatus(String tenantId, String status, Pageable pageable);

    /**
     * Find all ACTIVE profiles for a tenant (for merge detection)
     *
     * @param tenantId Tenant ID
     * @param pageable Pagination
     * @return Page of active profiles
     */
    Page<ProfileModel> findActiveProfiles(String tenantId, Pageable pageable);

    /**
     * Find all MERGED profiles
     *
     * @param tenantId Tenant ID
     * @param pageable Pagination
     * @return Page of merged profiles
     */
    Page<ProfileModel> findMergedProfiles(String tenantId, Pageable pageable);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BATCH OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Save multiple profiles
     *
     * @param profiles List of profiles to save
     * @return List of saved profiles
     */
    List<ProfileModel> saveAll(List<ProfileModel> profiles);

    /**
     * Find profiles by IDs
     *
     * @param ids List of profile IDs (format: tenantId|appId|userId)
     * @return List of found profiles
     */
    List<ProfileModel> findByIds(List<String> ids);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SEARCH / FILTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Search profiles by email
     *
     * @param tenantId Tenant ID
     * @param email Email to search
     * @return List of matching profiles
     */
    List<ProfileModel> findByEmail(String tenantId, String email);

    /**
     * Search profiles by phone
     *
     * @param tenantId Tenant ID
     * @param phone Phone to search
     * @return List of matching profiles
     */
    List<ProfileModel> findByPhone(String tenantId, String phone);

    /**
     * Search profiles by ID card
     *
     * @param tenantId Tenant ID
     * @param idcard ID card to search
     * @return List of matching profiles
     */
    List<ProfileModel> findByIdcard(String tenantId, String idcard);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATISTICS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Count profiles by status
     *
     * @param tenantId Tenant ID
     * @param status Profile status
     * @return Count
     */
    long countByStatus(String tenantId, String status);

    /**
     * Count total profiles for tenant
     *
     * @param tenantId Tenant ID
     * @return Count
     */
    long countByTenant(String tenantId);
}