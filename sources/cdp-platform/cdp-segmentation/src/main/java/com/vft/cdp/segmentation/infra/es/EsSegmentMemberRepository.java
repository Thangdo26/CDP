package com.vft.cdp.segmentation.infra.es;

import com.vft.cdp.segmentation.domain.repository.SegmentMemberRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class EsSegmentMemberRepository implements SegmentMemberRepository {

    private final SpringDataSegmentMemberEsRepository esRepo;

    @Override
    public void replaceMembers(String tenantId, String segmentId, List<String> userIds) {
        // Simple strategy: xoá hết & insert lại.
        // TODO: optimize bằng bulk delete + bulk insert.

        // Xoá toàn bộ member cũ
        // (tuỳ version Spring Data ES anh có thể dùng query delete hoặc scroll & delete)
        // Tạm thời bỏ qua implement chi tiết.

        // Insert mới
        List<SegmentMemberDocument> docs = userIds.stream()
                .map(userId -> {
                    SegmentMemberDocument doc = new SegmentMemberDocument();
                    doc.setId(buildId(tenantId, segmentId, userId));
                    doc.setTenantId(tenantId);
                    doc.setSegmentId(segmentId);
                    doc.setUserId(userId);
                    doc.setUpdatedAt(Instant.now());
                    return doc;
                })
                .toList();

        esRepo.saveAll(docs);
    }

    @Override
    public List<String> findMembers(String tenantId, String segmentId, int page, int size) {
        return esRepo.findAllByTenantIdAndSegmentId(
                        tenantId,
                        segmentId,
                        PageRequest.of(page, size)
                )
                .stream()
                .map(SegmentMemberDocument::getUserId)
                .toList();
    }

    @Override
    public long countMembers(String tenantId, String segmentId) {
        return esRepo.countByTenantIdAndSegmentId(tenantId, segmentId);
    }

    private String buildId(String tenantId, String segmentId, String userId) {
        return tenantId + "|" + segmentId + "|" + userId;
    }
}
