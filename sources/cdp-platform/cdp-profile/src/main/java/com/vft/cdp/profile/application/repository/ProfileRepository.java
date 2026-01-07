package com.vft.cdp.profile.application.repository;

import com.vft.cdp.profile.application.model.ProfileModel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE REPOSITORY (APPLICATION INTERFACE) - UPDATED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * NEW METHODS:
 * - findById(String profileId) - Find by unique profile ID
 * - findByIdcardGlobal(String idcard) - Find across all tenants
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface ProfileRepository {

    // ═══════════════════════════════════════════════════════════════
    // CRUD - Basic Operations
    // ═══════════════════════════════════════════════════════════════

    ProfileModel save(ProfileModel profile);

    /**
     * Find profile by unique profile_id
     * NEW: Used for looking up profile from mapping
     *
     * @param profileId Unique profile ID (idcard:xxx or uuid:xxx)
     */
    Optional<ProfileModel> findById(String profileId);

    /**
     * Find profile by tenant_id, app_id, user_id
     * LEGACY: Keep for backward compatibility
     */
    Optional<ProfileModel> find(String tenantId, String appId, String userId);

    void delete(String tenantId, String appId, String userId);

    boolean exists(String tenantId, String appId, String userId);

    // ═══════════════════════════════════════════════════════════════
    // Query by Status
    // ═══════════════════════════════════════════════════════════════

    Page<ProfileModel> findByStatus(String tenantId, String status, Pageable pageable);

    Page<ProfileModel> findActiveProfiles(String tenantId, Pageable pageable);

    Page<ProfileModel> findMergedProfiles(String tenantId, Pageable pageable);

    // ═══════════════════════════════════════════════════════════════
    // Batch Operations
    // ═══════════════════════════════════════════════════════════════

    List<ProfileModel> saveAll(List<ProfileModel> profiles);

    List<ProfileModel> findByIds(List<String> ids);

    // ═══════════════════════════════════════════════════════════════
    // Search by Identity Fields
    // ═══════════════════════════════════════════════════════════════

    /**
     * Find profiles by email within a tenant
     */
    List<ProfileModel> findByEmail(String tenantId, String email);

    /**
     * Find profiles by phone within a tenant
     */
    List<ProfileModel> findByPhone(String tenantId, String phone);

    /**
     * Find profiles by idcard within a tenant
     */
    List<ProfileModel> findByIdcard(String tenantId, String idcard);

    /**
     * Find profile by idcard GLOBALLY (across all tenants)
     * NEW: Used for deduplication in Merge Service
     *
     * @param idcard CCCD number
     * @return List of profiles with this idcard
     */
    List<ProfileModel> findByIdcardGlobal(String idcard);

    // ═══════════════════════════════════════════════════════════════
    // Statistics
    // ═══════════════════════════════════════════════════════════════

    long countByStatus(String tenantId, String status);

    long countByTenant(String tenantId);

    /**
     * Count total unique profiles (globally)
     */
    long countTotal();
}