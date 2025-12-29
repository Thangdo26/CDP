package com.vft.cdp.profile.infra.es;

import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * SPRING DATA ELASTICSEARCH REPOSITORY - MASTER PROFILE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 *  FIXED: Changed findByMergedIdsContaining to findByMergedProfileIdsContaining
 *
 * Reason: MasterProfileDocument has field 'mergedProfileIds', NOT 'mergedIds'
 * Spring Data derives query from method name, so must match exact field name
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Repository
public interface SpringDataMasterProfileRepository
        extends ElasticsearchRepository<MasterProfileDocument, String> {

    /**
     * Find master profile by masterId
     * Maps to field: masterId (String)
     */
    Optional<MasterProfileDocument> findByMasterId(String masterId);

    /**
     * Find master profiles that contain a specific merged profile ID
     *
     *  FIXED: Changed from findByMergedIdsContaining
     * Maps to field: mergedProfileIds (List<String>)
     */
    List<MasterProfileDocument> findByMergedProfileIdsContaining(String profileId);

    /**
     * Find master profiles by tenant and app
     * Maps to fields: tenantId (String), appId (String)
     */
    List<MasterProfileDocument> findByTenantIdAndAppId(String tenantId, String appId);

    /**
     * Find master profiles by tenant
     * Maps to field: tenantId (String)
     */
    List<MasterProfileDocument> findByTenantId(String tenantId);

    /**
     * Find master profiles by idcard
     *
     *  NEW: Used for smart merge
     * Maps to field: traits.idcard (String)
     */
    List<MasterProfileDocument> findByTraitsIdcard(String idcard);
}