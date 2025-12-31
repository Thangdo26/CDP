package com.vft.cdp.event.infra.es;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.time.Instant;
import java.util.Optional;

/**
 * Spring Data Elasticsearch Repository
 *
 * WHY EXTENDS ElasticsearchRepository?
 * - Auto-implementation: Spring Data generates code
 * - CRUD operations: save(), findById(), delete() etc.
 * - Custom queries: Method name → ES query
 *
 * SPRING DATA MAGIC:
 * You write interface → Spring generates implementation at runtime
 */
public interface SpringDataEventEsRepository
        extends ElasticsearchRepository<EventDocument, String> {

    /**
     * Find by tenant and profile
     *
     * METHOD NAME CONVENTION:
     * findBy + Field1 + And + Field2
     *
     * GENERATED QUERY:
     * {
     *   "query": {
     *     "bool": {
     *       "must": [
     *         {"term": {"tenant_id": "..."}},
     *         {"term": {"profile_id": "..."}}
     *       ]
     *     }
     *   }
     * }
     */
    Page<EventDocument> findByTenantIdAndProfileId(
            String tenantId,
            String profileId,
            Pageable pageable
    );

    /**
     * Find by tenant, profile, and time range
     *
     * METHOD NAME CONVENTION:
     * findBy + Field1 + And + Field2 + And + Field3 + Between
     *
     * GENERATED QUERY:
     * {
     *   "query": {
     *     "bool": {
     *       "must": [
     *         {"term": {"tenant_id": "..."}},
     *         {"term": {"profile_id": "..."}},
     *         {"range": {"normalized_time": {"gte": "...", "lte": "..."}}}
     *       ]
     *     }
     *   }
     * }
     */
    Page<EventDocument> findByTenantIdAndProfileIdAndNormalizedTimeBetween(
            String tenantId,
            String profileId,
            Instant startTime,
            Instant endTime,
            Pageable pageable
    );

    /**
     * Count events for profile
     */
    long countByTenantIdAndProfileId(String tenantId, String profileId);

    /**
     * Find by ID with Optional
     *
     * NOTE: This overrides default findById() to return Optional<EventDocument>
     * instead of Optional<EventDocument> (same, but explicit)
     */
    Optional<EventDocument> findById(String id);
}