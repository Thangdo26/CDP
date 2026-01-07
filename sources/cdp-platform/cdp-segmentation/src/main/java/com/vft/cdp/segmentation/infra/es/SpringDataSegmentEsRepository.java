package com.vft.cdp.segmentation.infra.es;

import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

import java.util.List;

public interface SpringDataSegmentEsRepository
        extends ElasticsearchRepository<SegmentDocument, String> {

    List<SegmentDocument> findAllByTenantId(String tenantId);

    SegmentDocument findByTenantIdAndId(String tenantId, String id);
}
