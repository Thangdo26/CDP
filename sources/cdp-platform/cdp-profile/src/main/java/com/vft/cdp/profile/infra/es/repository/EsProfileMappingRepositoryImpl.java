package com.vft.cdp.profile.infra.es.repository;

import com.vft.cdp.profile.application.repository.ProfileMappingRepository;
import com.vft.cdp.profile.infra.es.document.ProfileMappingDocument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.IndexOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * ES PROFILE MAPPING REPOSITORY IMPLEMENTATION - FIXED
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * FIXED: NullPointerException when doc is null
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Slf4j
@Repository
@RequiredArgsConstructor
public class EsProfileMappingRepositoryImpl implements ProfileMappingRepository {

    private final ElasticsearchOperations esOps;
    private volatile boolean indexChecked = false;

    @Override
    public Optional<String> findProfileId(String tenantId, String appId, String userId) {
        String id = ProfileMappingDocument.buildId(tenantId, appId, userId);

        try {
            ensureIndexExists();

            ProfileMappingDocument doc = esOps.get(id, ProfileMappingDocument.class);

            if (doc == null) {
                log.debug("❌ Mapping not found: {}", id);
                return Optional.empty();
            }

            // ✅ FIXED: Check if profileId is null
            if (doc.getProfileId() == null) {
                log.warn("⚠️ Mapping found but profileId is null: {}", id);
                return Optional.empty();
            }

            log.debug("✅ Found mapping: {} → {}", id, doc.getProfileId());
            return Optional.of(doc.getProfileId());

        } catch (Exception e) {
            log.error("❌ Error finding mapping: {}", id, e);
            return Optional.empty();
        }
    }

    @Override
    public boolean exists(String tenantId, String appId, String userId) {
        try {
            ensureIndexExists();
            String id = ProfileMappingDocument.buildId(tenantId, appId, userId);
            return esOps.exists(id, ProfileMappingDocument.class);
        } catch (Exception e) {
            log.error("❌ Error checking mapping existence", e);
            return false;
        }
    }

    @Override
    public void saveMapping(String tenantId, String appId, String userId, String profileId) {
        String id = ProfileMappingDocument.buildId(tenantId, appId, userId);

        log.info("💾 Saving mapping: {} → {}", id, profileId);

        try {
            ensureIndexExists();

            // Check if exists to preserve created_at
            ProfileMappingDocument existing = esOps.get(id, ProfileMappingDocument.class);

            ProfileMappingDocument doc;
            if (existing != null) {
                // Update existing
                doc = existing;
                doc.setProfileId(profileId);
                doc.setUpdatedAt(Instant.now());
            } else {
                // Create new
                doc = ProfileMappingDocument.create(tenantId, appId, userId, profileId);
            }

            esOps.save(doc);

            // ✅ FIXED: Refresh index to make mapping immediately searchable
            // This prevents race condition where profile is saved but mapping not yet indexed
            try {
                esOps.indexOps(ProfileMappingDocument.class).refresh();
                log.debug("🔄 Refreshed mapping index");
            } catch (Exception e) {
                log.warn("⚠️ Failed to refresh mapping index (non-critical): {}", e.getMessage());
            }

            log.info("✅ Mapping saved: {} → {}", id, profileId);

        } catch (Exception e) {
            log.error("❌ Error saving mapping: {} → {}", id, profileId, e);
            throw new RuntimeException("Failed to save mapping", e);
        }
    }

    @Override
    public void deleteMapping(String tenantId, String appId, String userId) {
        String id = ProfileMappingDocument.buildId(tenantId, appId, userId);

        log.info("🗑️ Deleting mapping: {}", id);

        try {
            ensureIndexExists();
            esOps.delete(id, ProfileMappingDocument.class);
            log.info("✅ Mapping deleted: {}", id);
        } catch (Exception e) {
            log.error("❌ Error deleting mapping: {}", id, e);
            throw new RuntimeException("Failed to delete mapping", e);
        }
    }

    @Override
    public List<String> findMappingsByProfileId(String profileId) {
        log.debug("🔍 Finding mappings for profile: {}", profileId);

        Criteria criteria = new Criteria("profile_id").is(profileId);
        CriteriaQuery query = new CriteriaQuery(criteria);

        SearchHits<ProfileMappingDocument> hits = esOps.search(
                query,
                ProfileMappingDocument.class
        );

        List<String> mappingIds = hits.stream()
                .map(SearchHit::getContent)
                .map(ProfileMappingDocument::getId)
                .collect(Collectors.toList());

        log.debug("✅ Found {} mappings for profile {}", mappingIds.size(), profileId);

        return mappingIds;
    }

    @Override
    public long countMappingsByProfileId(String profileId) {
        try {
            ensureIndexExists();

            Criteria criteria = new Criteria("profile_id").is(profileId);
            CriteriaQuery query = new CriteriaQuery(criteria);

            return esOps.count(query, ProfileMappingDocument.class);
        } catch (Exception e) {
            log.error("❌ Error counting mappings for profile: {}", profileId, e);
            return 0;
        }
    }

    /**
     * Ensure profile_mapping index exists, create if not
     */
    private void ensureIndexExists() {
        if (indexChecked) {
            return;
        }

        synchronized (this) {
            if (indexChecked) {
                return;
            }

            try {
                IndexOperations indexOps = esOps.indexOps(ProfileMappingDocument.class);

                if (!indexOps.exists()) {
                    log.info("Creating profile_mapping index...");
                    indexOps.create();
                    indexOps.putMapping();
                    log.info("✅ profile_mapping index created");
                } else {
                    log.debug("✓ profile_mapping index already exists");
                }

                indexChecked = true;
            } catch (Exception e) {
                log.error("❌ Failed to check/create profile_mapping index", e);
                throw new RuntimeException("Failed to initialize profile_mapping index", e);
            }
        }
    }
}