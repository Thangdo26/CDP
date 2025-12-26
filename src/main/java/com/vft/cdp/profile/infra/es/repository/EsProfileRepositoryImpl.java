package com.vft.cdp.profile.infra.es.repository;

import com.vft.cdp.profile.application.model.ProfileModel;
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
 * ES PROFILE REPOSITORY IMPLEMENTATION - FIXED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class EsProfileRepositoryImpl implements ProfileRepository {

    private final ElasticsearchOperations esOps;
    private final ProfileMapper mapper;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BASIC CRUD
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public ProfileModel save(ProfileModel model) {
        log.debug("💾 Saving profile: {}|{}|{}",
                model.getTenantId(), model.getAppId(), model.getUserId());

        ProfileDocument document;

        if (model instanceof Profile) {
            document = mapper.toDocument((Profile) model);
        } else {
            document = mapper.toDocument(model);
        }

        ProfileDocument saved = esOps.save(document);

        log.info("✅ Profile saved to ES: id={}", saved.getId());

        return mapper.toDomain(saved);
    }

    @Override
    public Optional<ProfileModel> find(String tenantId, String appId, String userId) {
        String id = mapper.buildId(tenantId, appId, userId);

        log.debug("🔍 Finding profile: id={}", id);

        ProfileDocument document = esOps.get(id, ProfileDocument.class);

        if (document == null) {
            log.debug("❌ Profile not found: {}", id);
            return Optional.empty();
        }

        log.debug("✅ Profile found: {}", id);

        return Optional.of(mapper.toDomain(document));
    }

    @Override
    public void delete(String tenantId, String appId, String userId) {
        String id = mapper.buildId(tenantId, appId, userId);

        log.info("🗑️  Deleting profile: id={}", id);

        esOps.delete(id, ProfileDocument.class);

        log.info("✅ Profile deleted from ES: {}", id);
    }

    @Override
    public boolean exists(String tenantId, String appId, String userId) {
        String id = mapper.buildId(tenantId, appId, userId);
        return esOps.exists(id, ProfileDocument.class);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY BY STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public Page<ProfileModel> findByStatus(String tenantId, String status, Pageable pageable) {
        log.debug("🔍 Finding profiles by status: tenant={}, status={}", tenantId, status);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("status").is(status);

        CriteriaQuery query = new CriteriaQuery(criteria).setPageable(pageable);

        var searchHits = esOps.search(query, ProfileDocument.class);

        List<ProfileModel> models = searchHits.stream()
                .map(SearchHit::getContent)
                .map(mapper::toDomain)
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

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BATCH OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public List<ProfileModel> saveAll(List<ProfileModel> models) {
        log.info("💾 Batch saving {} profiles", models.size());

        List<ProfileDocument> documents = models.stream()
                .map(model -> {
                    if (model instanceof Profile) {
                        return mapper.toDocument((Profile) model);
                    } else {
                        return mapper.toDocument(model);
                    }
                })
                .collect(Collectors.toList());

        Iterable<ProfileDocument> saved = esOps.save(documents);

        List<ProfileModel> result = new ArrayList<>();
        saved.forEach(doc -> result.add(mapper.toDomain(doc)));

        log.info("✅ Batch saved {} profiles", result.size());

        return result;
    }

    @Override
    public List<ProfileModel> findByIds(List<String> ids) {
        log.debug("🔍 Finding profiles by IDs: count={}", ids.size());

        // ✅ FIX: Simple loop instead of multiGet with IndexCoordinates
        List<ProfileModel> models = new ArrayList<>();

        for (String id : ids) {
            ProfileDocument doc = esOps.get(id, ProfileDocument.class);
            if (doc != null) {
                models.add(mapper.toDomain(doc));
            }
        }

        log.debug("✅ Found {} profiles", models.size());

        return models;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // SEARCH / FILTER
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public List<ProfileModel> findByEmail(String tenantId, String email) {
        log.debug("🔍 Finding profiles by email: tenant={}, email={}", tenantId, email);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.email").is(email.toLowerCase())
                .and("status").is("active");

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<ProfileModel> models = esOps.search(query, ProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(mapper::toDomain)
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
                .map(mapper::toDomain)
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
                .map(mapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} profiles with idcard={}", models.size(), idcard);

        return models;
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
}