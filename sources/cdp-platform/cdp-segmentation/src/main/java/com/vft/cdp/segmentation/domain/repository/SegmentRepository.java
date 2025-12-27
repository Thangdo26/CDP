package com.vft.cdp.segmentation.domain.repository;

import com.vft.cdp.segmentation.domain.model.Segment;

import java.util.List;
import java.util.Optional;

public interface SegmentRepository {

    Segment save(Segment segment);

    Optional<Segment> findById(String tenantId, String segmentId);

    List<Segment> findAllByTenant(String tenantId);
}
