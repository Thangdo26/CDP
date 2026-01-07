package com.vft.cdp.segmentation.infra.es;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;

public interface SpringDataSegmentMemberEsRepository
        extends ElasticsearchRepository<SegmentMemberDocument, String> {

    Page<SegmentMemberDocument> findAllByTenantIdAndSegmentId(
            String tenantId,
            String segmentId,
            Pageable pageable
    );

    long countByTenantIdAndSegmentId(String tenantId, String segmentId);
}
