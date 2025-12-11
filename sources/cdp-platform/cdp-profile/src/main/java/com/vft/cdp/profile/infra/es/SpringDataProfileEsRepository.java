package com.vft.cdp.profile.infra.es;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.Optional;

public interface SpringDataProfileEsRepository
        extends ElasticsearchRepository<ProfileDocument, String> {

    Optional<ProfileDocument> findByTenantIdAndProfileId(String tenantId, String profileId);
}
