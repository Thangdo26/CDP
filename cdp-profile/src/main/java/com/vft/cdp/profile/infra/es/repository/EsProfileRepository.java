package com.vft.cdp.profile.infra.es.repository;

import com.vft.cdp.profile.application.model.ProfileModel;
import com.vft.cdp.profile.application.repository.ProfileRepository;
import com.vft.cdp.profile.domain.Profile;
import com.vft.cdp.profile.infra.es.document.ProfileDocument;
import com.vft.cdp.profile.infra.es.mapper.ProfileInfraMapper;
import com.vft.cdp.profile.infra.es.model.ProfileModelImpl;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * ES PROFILE REPOSITORY
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Elasticsearch implementation of ProfileRepository.
 *
 * PATTERN: Repository Implementation (Infrastructure Layer)
 *
 * RESPONSIBILITY:
 * - Implements ProfileRepository interface (from Application layer)
 * - Handles ES-specific operations
 * - Returns ProfileModel interface (NOT ES documents)
 * - Converts between Domain/Model and ES Documents
 *
 * KEY DECISIONS:
 *
 * 1. What to return when loading from DB?
 *    Option A: Return Domain Profile entity
 *    Option B: Return ProfileModelImpl (adapter)
 *
 *    Choice: Option B (ProfileModelImpl)
 *    Why: Lightweight, no business logic overhead
 *
 * 2. What to accept when saving?
 *    - Accept ProfileModel interface
 *    - Check if it's Domain Profile (has business logic)
 *    - Or ProfileModelImpl (just data)
 *    - Convert both to ES Document
 *
 * 3. How to handle updates?
 *    - If saving Domain Profile → preserve business logic
 *    - If saving ProfileModel → just save data
 *
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Repository
@RequiredArgsConstructor
public class EsProfileRepository implements ProfileRepository {

    private final ElasticsearchOperations esOps;
    private final ProfileInfraMapper mapper;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BASIC CRUD
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public ProfileModel save(ProfileModel model) {
        // Convert Model → ES Document
        ProfileDocument document;

        if (model instanceof Profile) {
            // Domain entity with business logic
            document = mapper.toDocument((Profile) model);
        } else {
            // Generic ProfileModel (reconstruct from model data)
            document = mapper.toDocument(model);
        }

        // Save to ES
        ProfileDocument saved = esOps.save(document);

        // Return as ProfileModel (lightweight adapter)
        return new ProfileModelImpl(saved);
    }

    @Override
    public Optional<ProfileModel> find(String tenantId, String appId, String userId) {
        String id = buildId(tenantId, appId, userId);

        // Load from ES
        ProfileDocument document = esOps.get(id, ProfileDocument.class);

        if (document == null) {
            return Optional.empty();
        }

        // Return as ProfileModel adapter (NOT Domain entity)
        // Why? Loading doesn't need business logic
        return Optional.of(new ProfileModelImpl(document));
    }

    @Override
    public void delete(String tenantId, String appId, String userId) {
        String id = buildId(tenantId, appId, userId);
        esOps.delete(id, ProfileDocument.class);
    }

    @Override
    public boolean exists(String tenantId, String appId, String userId) {
        String id = buildId(tenantId, appId, userId);
        return esOps.exists(id, ProfileDocument.class);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY BY STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public Page<ProfileModel> findByStatus(String tenantId, String status, Pageable pageable) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("status").is(status);

        CriteriaQuery query = new CriteriaQuery(criteria).setPageable(pageable);

        var searchHits = esOps.search(query, ProfileDocument.class);

        List<ProfileModel> models = searchHits.stream()
                .map(SearchHit::getContent)
                .map(ProfileModelImpl::new)  // Document → Model adapter
                .collect(Collectors.toList());

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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BATCH OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public List<ProfileModel> saveAll(List<ProfileModel> models) {
        // Convert Models → Documents
        List<ProfileDocument> documents = models.stream()
                .map(model -> {
                    if (model instanceof Profile) {
                        return mapper.toDocument((Profile) model);
                    } else {
                        return mapper.toDocument(model);
                    }
                })
                .collect(Collectors.toList());

        // Bulk save
        Iterable<ProfileDocument> saved = esOps.save(documents);

        // Convert back to Models
        return ((List<ProfileDocument>) saved).stream()
                .map(ProfileModelImpl::new)
                .collect(Collectors.toList());
    }

    @Override
    public List<ProfileModel> findByIds(List<String> ids) {
        var multiGetQuery = org.springframework.data.elasticsearch.core.query.Query
                .multiGetQuery(ids, ProfileDocument.class);

        return esOps.multiGet(multiGetQuery, ProfileDocument.class).stream()
                .map(ProfileModelImpl::new)
                .collect(Collectors.toList());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SEARCH / FILTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public List<ProfileModel> findByEmail(String tenantId, String email) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.email").is(email)
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        return esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileModelImpl::new)
                .collect(Collectors.toList());
    }

    @Override
    public List<ProfileModel> findByPhone(String tenantId, String phone) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.phone").is(phone)
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        return esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileModelImpl::new)
                .collect(Collectors.toList());
    }

    @Override
    public List<ProfileModel> findByIdcard(String tenantId, String idcard) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.idcard").is(idcard)
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        return esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(ProfileModelImpl::new)
                .collect(Collectors.toList());
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATISTICS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // UTILITIES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String buildId(String tenantId, String appId, String userId) {
        return String.format("%s|%s|%s", tenantId, appId, userId);
    }
}