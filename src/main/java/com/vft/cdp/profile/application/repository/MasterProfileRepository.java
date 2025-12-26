package com.vft.cdp.profile.application.repository;

import com.vft.cdp.profile.application.model.MasterProfileModel;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.util.List;
import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE REPOSITORY INTERFACE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Application Layer - Repository abstraction
 *
 * PATTERN: Dependency Inversion
 * - Interface defined in Application layer
 * - Implementation in Infrastructure layer
 * - Returns MasterProfileModel interface (NOT ES documents)
 *
 * PURPOSE: Enable switching between different storage implementations
 * (Elasticsearch, MongoDB, PostgreSQL, etc.) without changing
 * Application or Domain layer code
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
public interface MasterProfileRepository {

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BASIC CRUD
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Save or update master profile
     */
    MasterProfileModel save(MasterProfileModel model);

    /**
     * Find master profile by composite key
     */
    Optional<MasterProfileModel> find(String tenantId, String appId, String masterId);

    /**
     * Delete master profile
     */
    void delete(String tenantId, String appId, String masterId);

    /**
     * Check if master profile exists
     */
    boolean exists(String tenantId, String appId, String masterId);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Find all master profiles for tenant + app (with pagination)
     */
    Page<MasterProfileModel> findAll(String tenantId, String appId, Pageable pageable);

    /**
     * Find master profiles by email
     */
    List<MasterProfileModel> findByEmail(String tenantId, String email);

    /**
     * Find master profiles by phone
     */
    List<MasterProfileModel> findByPhone(String tenantId, String phone);

    /**
     * Find master profiles by ID card
     */
    List<MasterProfileModel> findByIdcard(String tenantId, String idcard);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BATCH OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Batch save master profiles
     */
    List<MasterProfileModel> saveAll(List<MasterProfileModel> models);

    /**
     * Find master profiles by IDs
     */
    List<MasterProfileModel> findByIds(List<String> ids);

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATISTICS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    /**
     * Count master profiles by tenant
     */
    long countByTenant(String tenantId);
}