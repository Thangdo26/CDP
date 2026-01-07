package com.vft.cdp.profile.infra.es.repository;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.application.repository.ProfileMappingRepository;
import com.vft.cdp.profile.application.repository.ProfileRepository;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import com.vft.cdp.profile.infra.es.mapper.ProfileMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * ES PROFILE REPOSITORY IMPLEMENTATION - UPDATED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * NEW METHODS:
 * - findById(String profileId) - Find by unique profile ID
 * - findByIdcardGlobal(String idcard) - Find across all tenants
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class EsProfileRepositoryImpl implements ProfileRepository {

    private final ElasticsearchOperations esOps;
    private final ProfileMappingRepository mappingRepository;

    // ═══════════════════════════════════════════════════════════════
    // BASIC CRUD
    // ═══════════════════════════════════════════════════════════════

    @Override
    public ProfileModel save(ProfileModel model) {
        // ✅ Step 1: Extract profile ID
        String profileId = extractProfileId(model);

        log.info("💾 Saving profile: id={}", profileId);

        // ✅ Step 2: Rebuild users[] from mappings
        List<ProfileModel.UserIdentityModel> users = rebuildUsersFromMappings(profileId);

        // ✅ Step 3: Convert to document
        ProfileDocument document;
        if (model instanceof Profile) {
            document = ProfileMapper.toDocument((Profile) model);
        } else {
            document = ProfileMapper.toDocument(model);
        }

        // ✅ Step 4: Set computed users[]
        document.setUsers(mapUsersToDocument(users));

        log.debug("🔄 Set {} users to profile document", users.size());

        // ✅ Step 5: Save to ES
        ProfileDocument saved = esOps.save(document);

        log.info("✅ Profile saved with {} users: id={}", users.size(), saved.getId());

        // ✅ Step 6: Return with computed users
        Profile domain = ProfileMapper.toDomain(saved);
        return domain;
    }

    // Helper method
    private List<ProfileDocument.UserIdentity> mapUsersToDocument(
            List<ProfileModel.UserIdentityModel> users
    ) {
        if (users == null || users.isEmpty()) {
            return new ArrayList<>();
        }

        return users.stream()
                .map(u -> ProfileDocument.UserIdentity.builder()
                        .appId(u.getAppId())
                        .userId(u.getUserId())
                        .build())
                .collect(Collectors.toList());
    }

    // Helper to extract profile ID
    private String extractProfileId(ProfileModel model) {
        // Profile ID is in userId field (ES document _id)
        String userId = model.getUserId();

        // If has idcard, prefer it
        if (model.getTraits() != null && model.getTraits().getIdcard() != null) {
            String idcard = model.getTraits().getIdcard();
            if (!idcard.isBlank()) {
                return idcard;
            }
        }

        return userId;
    }

    /**
     * NEW: Find profile by unique profile_id
     *
     * Profile ID formats:
     * - idcard:123456789012 (if idcard exists)
     * - uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (if no idcard)
     * - tenant_id|app_id|user_id (legacy format)
     */
    @Override
    public Optional<ProfileModel> findById(String profileId) {

        ProfileDocument document = esOps.get(profileId, ProfileDocument.class);

        if (document != null) {
            log.debug("✅ Found profile by _id: {}", profileId);
            return Optional.of(ProfileMapper.toDomain(document));
        }

        log.debug("❌ Profile not found: {}", profileId);
        return Optional.empty();
    }

    private Optional<ProfileModel> findFirstByIdcard(String idcard) {

        Criteria criteria = new Criteria("traits.idcard").is(idcard)
                .and("status").is("ACTIVE"); // 🔥 FIX CASE-SENSITIVE

        CriteriaQuery query = new CriteriaQuery(criteria);

        var searchHits = esOps.search(query, ProfileDocument.class);

        return searchHits.stream()
                .findFirst()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain);
    }

    /**
     * LEGACY: Find profile by tenant_id, app_id, user_id
     */
    @Override
    public Optional<ProfileModel> find(String tenantId, String appId, String userId) {
        String id = ProfileMapper.buildMappingId(tenantId, appId, userId);

        ProfileDocument document = esOps.get(id, ProfileDocument.class);

        if (document == null) {
            return Optional.empty();
        }

        return Optional.of(ProfileMapper.toDomain(document));
    }

    @Override
    public void delete(String tenantId, String appId, String userId) {
        String id = ProfileMapper.buildMappingId(tenantId, appId, userId);

        log.info("🗑️ Deleting profile: id={}", id);

        esOps.delete(id, ProfileDocument.class);

        log.info("✅ Profile deleted from ES: {}", id);
    }

    @Override
    public boolean exists(String tenantId, String appId, String userId) {
        String id = ProfileMapper.buildMappingId(tenantId, appId, userId);
        return esOps.exists(id, ProfileDocument.class);
    }

    // ═══════════════════════════════════════════════════════════════
    // QUERY BY STATUS
    // ═══════════════════════════════════════════════════════════════

    @Override
    public Page<ProfileModel> findByStatus(String tenantId, String status, Pageable pageable) {
        log.debug("🔍 Finding profiles by status: tenant={}, status={}", tenantId, status);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("status").is(status);

        CriteriaQuery query = new CriteriaQuery(criteria).setPageable(pageable);

        var searchHits = esOps.search(query, ProfileDocument.class);

        List<ProfileModel> models = searchHits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} profiles with status={}", models.size(), status);

        return new PageImpl<>(models, pageable, searchHits.getTotalHits());
    }

    @Override
    public Page<ProfileModel> findActiveProfiles(String tenantId, Pageable pageable) {
        return findByStatus(tenantId, "active", pageable);
    }

    @Override
    public Page<ProfileModel> findMergedProfiles(String tenantId, Pageable pageable) {
        return findByStatus(tenantId, "merged", pageable);
    }

    // ═══════════════════════════════════════════════════════════════
    // BATCH OPERATIONS
    // ═══════════════════════════════════════════════════════════════

    @Override
    public List<ProfileModel> saveAll(List<ProfileModel> models) {
        log.info("💾 Batch saving {} profiles", models.size());

        List<ProfileDocument> documents = models.stream()
                .map(model -> {
                    if (model instanceof Profile) {
                        return ProfileMapper.toDocument((Profile) model);
                    } else {
                        return ProfileMapper.toDocument(model);
                    }
                })
                .collect(Collectors.toList());

        Iterable<ProfileDocument> saved = esOps.save(documents);

        List<ProfileModel> result = new ArrayList<>();
        saved.forEach(doc -> result.add(ProfileMapper.toDomain(doc)));

        log.info("✅ Batch saved {} profiles", result.size());

        return result;
    }

    @Override
    public List<ProfileModel> findByIds(List<String> ids) {
        log.debug("🔍 Finding profiles by IDs: count={}", ids.size());

        List<ProfileModel> models = new ArrayList<>();

        for (String id : ids) {
            ProfileDocument doc = esOps.get(id, ProfileDocument.class);
            if (doc != null) {
                models.add(ProfileMapper.toDomain(doc));
            }
        }

        log.debug("✅ Found {} profiles", models.size());

        return models;
    }

    // ═══════════════════════════════════════════════════════════════
    // SEARCH BY IDENTITY FIELDS
    // ═══════════════════════════════════════════════════════════════

    @Override
    public List<ProfileModel> findByEmail(String tenantId, String email) {
        log.debug("🔍 Finding profiles by email: tenant={}, email={}", tenantId, email);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.email").is(email.toLowerCase())
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<ProfileModel> models = esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} profiles with email={}", models.size(), email);

        return models;
    }

    @Override
    public List<ProfileModel> findByPhone(String tenantId, String phone) {
        log.debug("🔍 Finding profiles by phone: tenant={}, phone={}", tenantId, phone);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.phone").is(phone)
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<ProfileModel> models = esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} profiles with phone={}", models.size(), phone);

        return models;
    }

    @Override
    public List<ProfileModel> findByIdcard(String tenantId, String idcard) {
        log.debug("🔍 Finding profiles by idcard: tenant={}, idcard={}", tenantId, idcard);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.idcard").is(idcard)
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<ProfileModel> models = esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} profiles with idcard={}", models.size(), idcard);

        return models;
    }

    /**
     * NEW: Find profile by idcard GLOBALLY (across all tenants)
     * Used for deduplication in Merge Service
     */
    @Override
    public List<ProfileModel> findByIdcardGlobal(String idcard) {
        log.debug("🔍 Finding profiles by idcard GLOBALLY: idcard={}", idcard);

        // Search across ALL tenants - no tenant_id filter
        Criteria criteria = new Criteria("traits.idcard").is(idcard)
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<ProfileModel> models = esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} profiles GLOBALLY with idcard={}", models.size(), idcard);

        return models;
    }

    // ═══════════════════════════════════════════════════════════════
    // STATISTICS
    // ═══════════════════════════════════════════════════════════════

    @Override
    public long countByStatus(String tenantId, String status) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("status").is(status);

        CriteriaQuery query = new CriteriaQuery(criteria);

        return esOps.count(query, ProfileDocument.class);
    }

    @Override
    public long countByTenant(String tenantId) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId);
        CriteriaQuery query = new CriteriaQuery(criteria);

        return esOps.count(query, ProfileDocument.class);
    }

    /**
     * NEW: Count total unique profiles (globally)
     */
    @Override
    public long countTotal() {
        CriteriaQuery query = new CriteriaQuery(new Criteria("status").is("active"));
        return esOps.count(query, ProfileDocument.class);
    }

    @Override
    public List<ProfileModel.UserIdentityModel> rebuildUsersFromMappings(String profileId) {
        log.info("🔄 Rebuilding users[] from mappings for profile: {}", profileId);

        List<String> mappingIds = mappingRepository.findMappingsByProfileId(profileId);

        // ✅ ADD THIS LOG
        log.info("Found {} mapping IDs: {}", mappingIds.size(), mappingIds);

        if (mappingIds.isEmpty()) {
            log.warn("⚠️ No mappings found for profile: {}", profileId);
            return new ArrayList<>();
        }

        List<ProfileModel.UserIdentityModel> users = new ArrayList<>();

        for (String mappingId : mappingIds) {
            String[] parts = mappingId.split("\\|");
            if (parts.length == 3) {
                String appId = parts[1];
                String userId = parts[2];

                users.add(Profile.UserIdentity.builder()
                        .appId(appId)
                        .userId(userId)
                        .build());

                log.info("  ✅ Added user: {}|{}", appId, userId);
            }
        }

        log.info("✅ Rebuilt {} users for profile {}", users.size(), profileId);
        return users;
    }

}