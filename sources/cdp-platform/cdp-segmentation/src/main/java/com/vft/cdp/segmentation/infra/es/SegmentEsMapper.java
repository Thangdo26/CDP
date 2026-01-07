package com.vft.cdp.segmentation.infra.es;

import com.vft.cdp.segmentation.domain.model.Segment;
import com.vft.cdp.segmentation.domain.model.SegmentDefinition;
import com.vft.cdp.segmentation.domain.model.SegmentStatus;

public final class SegmentEsMapper {

    private SegmentEsMapper() {
        // utility class
    }

    public static SegmentDocument toDocument(Segment segment) {
        SegmentDocument doc = new SegmentDocument();
        doc.setId(segment.getId());
        doc.setTenantId(segment.getTenantId());
        doc.setName(segment.getName());
        doc.setDescription(segment.getDescription());
        String json = segment.getDefinition().getRawJson().toString();
        doc.setDefinitionJson(json);
        doc.setStatus(segment.getStatus().name());
        doc.setCreatedAt(segment.getCreatedAt());
        doc.setUpdatedAt(segment.getUpdatedAt());
        doc.setLastBuiltAt(segment.getLastBuiltAt());
        doc.setEstimatedSize(segment.getEstimatedSize());
        return doc;
    }

    public static Segment toDomain(SegmentDocument doc) {
        if (doc == null) {
            return null;
        }

        SegmentDefinition def = new SegmentDefinition(doc.getDefinitionJson());
        SegmentStatus status = SegmentStatus.valueOf(doc.getStatus());

        return Segment.builder()
                .id(doc.getId())
                .tenantId(doc.getTenantId())
                .name(doc.getName())
                .description(doc.getDescription())
                .definition(def)
                .status(status)
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .lastBuiltAt(doc.getLastBuiltAt())
                .estimatedSize(doc.getEstimatedSize())
                .build();
    }
}
