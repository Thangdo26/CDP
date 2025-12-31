package com.vft.cdp.segmentation.infra.es;

import com.vft.cdp.segmentation.domain.model.Segment;
import com.vft.cdp.segmentation.domain.repository.SegmentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
@RequiredArgsConstructor
public class EsSegmentRepository implements SegmentRepository {

    private final SpringDataSegmentEsRepository esRepo;

    @Override
    public Segment save(Segment segment) {
        SegmentDocument doc = SegmentEsMapper.toDocument(segment);
        SegmentDocument saved = esRepo.save(doc);
        return SegmentEsMapper.toDomain(saved);
    }

    @Override
    public Optional<Segment> findById(String tenantId, String segmentId) {
        SegmentDocument doc = esRepo.findByTenantIdAndId(tenantId, segmentId);
        return Optional.ofNullable(SegmentEsMapper.toDomain(doc));
    }

    @Override
    public List<Segment> findAllByTenant(String tenantId) {
        return esRepo.findAllByTenantId(tenantId)
                .stream()
                .map(SegmentEsMapper::toDomain)
                .toList();
    }
}
