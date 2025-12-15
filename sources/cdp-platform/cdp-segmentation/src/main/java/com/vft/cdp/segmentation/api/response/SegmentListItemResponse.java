package com.vft.cdp.segmentation.api.response;

import com.vft.cdp.segmentation.application.dto.SegmentDto;
import com.vft.cdp.segmentation.domain.model.SegmentStatus;

import java.time.Instant;

public record SegmentListItemResponse(
        String id,
        String name,
        String description,
        SegmentStatus status,
        Instant updatedAt,
        Long estimatedSize
) {
    public static SegmentListItemResponse fromDto(SegmentDto dto) {
        return new SegmentListItemResponse(
                dto.id(),
                dto.name(),
                dto.description(),
                dto.status(),
                dto.updatedAt(),
                dto.estimatedSize()
        );
    }
}
