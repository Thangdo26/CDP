package com.vft.cdp.profile.infra.es;

import com.vft.cdp.profile.infra.es.document.MasterProfileDocument;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface SpringDataMasterProfileRepository
        extends ElasticsearchRepository<MasterProfileDocument, String> {

    /**
     * Find master profiles by tenant
     */
    List<MasterProfileDocument> findByTenantId(String tenantId);

    /**
     * Find by merged IDs (check if profile already merged)
     */
    List<MasterProfileDocument> findByMergedIdsContaining(String profileId);
}