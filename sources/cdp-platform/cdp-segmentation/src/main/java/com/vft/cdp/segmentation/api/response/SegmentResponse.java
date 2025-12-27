package com.vft.cdp.segmentation.api.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.vft.cdp.segmentation.application.dto.SegmentDto;
import com.vft.cdp.segmentation.domain.model.SegmentStatus;

import java.time.Instant;

public record SegmentResponse(
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
    public static SegmentResponse fromDto(SegmentDto dto) {
        return new SegmentResponse(
                dto.id(),
                dto.tenantId(),
                dto.name(),
                dto.description(),
                dto.definitionJson(),
                dto.status(),
                dto.createdAt(),
                dto.updatedAt(),
                dto.lastBuiltAt(),
                dto.estimatedSize()
        );
    }
}
