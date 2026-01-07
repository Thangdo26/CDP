package com.vft.cdp.segmentation.application.dto;

import com.fasterxml.jackson.databind.JsonNode;
import com.vft.cdp.common.json.JsonUtils;
import com.vft.cdp.segmentation.domain.model.Segment;
import com.vft.cdp.segmentation.domain.model.SegmentStatus;

import java.time.Instant;

public record SegmentDto(
        String id,
        String tenantId,
        String name,
        String description,
        JsonNode definitionJson,
        SegmentStatus status,
        Instant createdAt,
        Instant updatedAt,
        Instant lastBuiltAt,
        Long estimatedSize
) {
    public static SegmentDto fromDomain(Segment segment) {
        return new SegmentDto(
                segment.getId(),
                segment.getTenantId(),
                segment.getName(),
                segment.getDescription(),
                JsonUtils.readTree(segment.getDefinition().getRawJson()),
                segment.getStatus(),
                segment.getCreatedAt(),
                segment.getUpdatedAt(),
                segment.getLastBuiltAt(),
                segment.getEstimatedSize()
        );
    }
}
