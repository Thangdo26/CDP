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
 */
public interface ProfileRepository {

    // CRUD
    ProfileModel save(ProfileModel profile);
    Optional<ProfileModel> find(String tenantId, String appId, String userId);
    void delete(String tenantId, String appId, String userId);
    boolean exists(String tenantId, String appId, String userId);

    // Query by status
    Page<ProfileModel> findByStatus(String tenantId, String status, Pageable pageable);
    Page<ProfileModel> findActiveProfiles(String tenantId, Pageable pageable);
    Page<ProfileModel> findMergedProfiles(String tenantId, Pageable pageable);

    // Batch operations
    List<ProfileModel> saveAll(List<ProfileModel> profiles);
    List<ProfileModel> findByIds(List<String> ids);

    // Search
    List<ProfileModel> findByEmail(String tenantId, String email);
    List<ProfileModel> findByPhone(String tenantId, String phone);
    List<ProfileModel> findByIdcard(String tenantId, String idcard);

    // Statistics
    long countByStatus(String tenantId, String status);
    long countByTenant(String tenantId);
}