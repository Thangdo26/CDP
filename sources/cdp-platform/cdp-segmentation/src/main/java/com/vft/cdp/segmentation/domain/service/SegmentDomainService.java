package com.vft.cdp.segmentation.domain.service;

import com.vft.cdp.segmentation.domain.model.Segment;
import com.vft.cdp.segmentation.domain.model.SegmentDefinition;

public class SegmentDomainService {

    public void validateDefinition(SegmentDefinition definition) {
        // TODO: parse JSON DSL, validate structure
        // Tạm thời để trống
    }

    public boolean canActivate(Segment segment) {
        // Ví dụ: chỉ cho ACTIVE nếu đã build xong
        return segment.getLastBuiltAt() != null;
    }
}
