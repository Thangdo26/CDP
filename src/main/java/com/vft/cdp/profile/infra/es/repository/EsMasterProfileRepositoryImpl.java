package com.vft.cdp.profile.infra.es.repository;

import com.vft.cdp.profile.application.model.MasterProfileModel;
import com.vft.cdp.profile.application.repository.MasterProfileRepository;
import com.vft.cdp.profile.domain.MasterProfile;
import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
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
 * ES MASTER PROFILE REPOSITORY IMPLEMENTATION
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Implements MasterProfileRepository interface from Application layer
 * Uses static MasterProfileMapper utility methods
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class EsMasterProfileRepositoryImpl implements MasterProfileRepository {

    private final ElasticsearchOperations esOps;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BASIC CRUD
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public MasterProfileModel save(MasterProfileModel model) {
        log.debug("💾 Saving master profile: {}|{}|{}",
                model.getTenantId(),
                model.getAppId().isEmpty() ? "N/A" : model.getAppId().get(0),
                model.getProfileId());

        // Convert to domain entity for mapping
        MasterProfile domain;
        if (model instanceof MasterProfile) {
            domain = (MasterProfile) model;
        } else {
            // Reconstruct domain from model interface
            domain = MasterProfile.builder()
                    .profileId(model.getProfileId())
                    .tenantId(model.getTenantId())
                    .appId(model.getAppId())
                    .status(model.getStatus())
                    .isAnonymous(model.isAnonymous())
                    .deviceId(model.getDeviceId())
                    .mergedIds(model.getMergedIds())
                    .traits(convertTraitsToDomain(model.getTraits()))
                    .segments(model.getSegments())
                    .scores(model.getScores())
                    .consents(convertConsentsToDomain(model.getConsents()))
                    .createdAt(model.getCreatedAt())
                    .updatedAt(model.getUpdatedAt())
                    .firstSeenAt(model.getFirstSeenAt())
                    .lastSeenAt(model.getLastSeenAt())
                    .sourceSystems(model.getSourceSystems())
                    .version(model.getVersion())
                    .build();
        }

        MasterProfileDocument document = MasterProfileMapper.toDocument(domain);
        MasterProfileDocument saved = esOps.save(document);

        log.info("✅ Master profile saved to ES: id={}", saved.getId());

        return MasterProfileMapper.toDomain(saved);
    }

    @Override
    public Optional<MasterProfileModel> find(String tenantId, String appId, String masterId) {
        String id = buildId(tenantId, appId, masterId);

        log.debug("🔍 Finding master profile: id={}", id);

        MasterProfileDocument document = esOps.get(id, MasterProfileDocument.class);

        if (document == null) {
            log.debug("❌ Master profile not found: {}", id);
            return Optional.empty();
        }

        log.debug("✅ Master profile found: {}", id);

        return Optional.of(MasterProfileMapper.toDomain(document));
    }

    @Override
    public void delete(String tenantId, String appId, String masterId) {
        String id = buildId(tenantId, appId, masterId);

        log.info("🗑️ Deleting master profile: id={}", id);

        esOps.delete(id, MasterProfileDocument.class);

        log.info("✅ Master profile deleted from ES: {}", id);
    }

    @Override
    public boolean exists(String tenantId, String appId, String masterId) {
        String id = buildId(tenantId, appId, masterId);
        return esOps.exists(id, MasterProfileDocument.class);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // QUERY OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public Page<MasterProfileModel> findAll(String tenantId, String appId, Pageable pageable) {
        log.debug("🔍 Finding all master profiles: tenant={}, app={}", tenantId, appId);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("app_id").is(appId);

        CriteriaQuery query = new CriteriaQuery(criteria).setPageable(pageable);

        var searchHits = esOps.search(query, MasterProfileDocument.class);

        List<MasterProfileModel> models = searchHits.stream()
                .map(SearchHit::getContent)
                .map(MasterProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} master profiles", models.size());

        return new PageImpl<>(models, pageable, searchHits.getTotalHits());
    }

    @Override
    public List<MasterProfileModel> findByEmail(String tenantId, String email) {
        log.debug("🔍 Finding master profiles by email: tenant={}, email={}", tenantId, email);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.email").is(email.toLowerCase());

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<MasterProfileModel> models = esOps.search(query, MasterProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(MasterProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} master profiles with email={}", models.size(), email);

        return models;
    }

    @Override
    public List<MasterProfileModel> findByPhone(String tenantId, String phone) {
        log.debug("🔍 Finding master profiles by phone: tenant={}, phone={}", tenantId, phone);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.phone").is(phone);

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<MasterProfileModel> models = esOps.search(query, MasterProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(MasterProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} master profiles with phone={}", models.size(), phone);

        return models;
    }

    @Override
    public List<MasterProfileModel> findByIdcard(String tenantId, String idcard) {
        log.debug("🔍 Finding master profiles by idcard: tenant={}, idcard={}", tenantId, idcard);

        Criteria criteria = new Criteria("tenant_id").is(tenantId)
                .and("traits.idcard").is(idcard);

        CriteriaQuery query = new CriteriaQuery(criteria);

        List<MasterProfileModel> models = esOps.search(query, MasterProfileDocument.class).stream()
                .map(SearchHit::getContent)
                .map(MasterProfileMapper::toDomain)
                .collect(Collectors.toList());

        log.debug("✅ Found {} master profiles with idcard={}", models.size(), idcard);

        return models;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // BATCH OPERATIONS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public List<MasterProfileModel> saveAll(List<MasterProfileModel> models) {
        log.info("💾 Batch saving {} master profiles", models.size());

        List<MasterProfileDocument> documents = models.stream()
                .map(model -> {
                    if (model instanceof MasterProfile) {
                        return MasterProfileMapper.toDocument((MasterProfile) model);
                    } else {
                        // Reconstruct domain from model
                        MasterProfile domain = MasterProfile.builder()
                                .profileId(model.getProfileId())
                                .tenantId(model.getTenantId())
                                .appId(model.getAppId())
                                .status(model.getStatus())
                                .isAnonymous(model.isAnonymous())
                                .deviceId(model.getDeviceId())
                                .mergedIds(model.getMergedIds())
                                .traits(convertTraitsToDomain(model.getTraits()))
                                .segments(model.getSegments())
                                .scores(model.getScores())
                                .consents(convertConsentsToDomain(model.getConsents()))
                                .createdAt(model.getCreatedAt())
                                .updatedAt(model.getUpdatedAt())
                                .firstSeenAt(model.getFirstSeenAt())
                                .lastSeenAt(model.getLastSeenAt())
                                .sourceSystems(model.getSourceSystems())
                                .version(model.getVersion())
                                .build();
                        return MasterProfileMapper.toDocument(domain);
                    }
                })
                .collect(Collectors.toList());

        Iterable<MasterProfileDocument> saved = esOps.save(documents);

        List<MasterProfileModel> result = new ArrayList<>();
        saved.forEach(doc -> result.add(MasterProfileMapper.toDomain(doc)));

        log.info("✅ Batch saved {} master profiles", result.size());

        return result;
    }

    @Override
    public List<MasterProfileModel> findByIds(List<String> ids) {
        log.debug("🔍 Finding master profiles by IDs: count={}", ids.size());

        List<MasterProfileModel> models = new ArrayList<>();

        for (String id : ids) {
            MasterProfileDocument doc = esOps.get(id, MasterProfileDocument.class);
            if (doc != null) {
                models.add(MasterProfileMapper.toDomain(doc));
            }
        }

        log.debug("✅ Found {} master profiles", models.size());

        return models;
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATISTICS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Override
    public long countByTenant(String tenantId) {
        Criteria criteria = new Criteria("tenant_id").is(tenantId);
        CriteriaQuery query = new CriteriaQuery(criteria);

        return esOps.count(query, MasterProfileDocument.class);
    }

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // HELPER METHODS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    private String buildId(String tenantId, String appId, String masterId) {
        return tenantId + "|" + appId + "|" + masterId;
    }

    private MasterProfile.MasterTraits convertTraitsToDomain(MasterProfileModel.MasterTraitsModel traits) {
        if (traits == null) return null;

        if (traits instanceof MasterProfile.MasterTraits) {
            return (MasterProfile.MasterTraits) traits;
        }

        return MasterProfile.MasterTraits.builder()
                .email(traits.getEmail())
                .phone(traits.getPhone())
                .userId(traits.getUserId())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .country(traits.getCountry())
                .city(traits.getCity())
                .address(traits.getAddress())
                .lastPurchaseAmount(traits.getLastPurchaseAmount())
                .lastPurchaseAt(traits.getLastPurchaseAt())
                .build();
    }

    private java.util.Map<String, MasterProfile.Consent> convertConsentsToDomain(
            java.util.Map<String, MasterProfileModel.ConsentModel> consents) {

        if (consents == null) return new java.util.HashMap<>();

        java.util.Map<String, MasterProfile.Consent> result = new java.util.HashMap<>();

        for (java.util.Map.Entry<String, MasterProfileModel.ConsentModel> entry : consents.entrySet()) {
            if (entry.getValue() instanceof MasterProfile.Consent) {
                result.put(entry.getKey(), (MasterProfile.Consent) entry.getValue());
            } else {
                MasterProfile.Consent consent = MasterProfile.Consent.builder()
                        .status(entry.getValue().getStatus())
                        .updatedAt(entry.getValue().getUpdatedAt())
                        .source(entry.getValue().getSource())
                        .build();
                result.put(entry.getKey(), consent);
            }
        }

        return result;
    }
}