package com.vft.cdp.segmentation.domain.repository;

import java.util.List;

/**
 * Lưu danh sách user trong segment.
 * Implementation sẽ lưu vào ES index cdp-segment-members-<tenantId>-<segmentId>.
 */
public interface SegmentMemberRepository {

    void replaceMembers(String tenantId, String segmentId, List<String> userIds);

    List<String> findMembers(String tenantId, String segmentId, int page, int size);

    long countMembers(String tenantId, String segmentId);
}
