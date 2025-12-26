package com.vft.cdp.segmentation.domain.model;

import java.util.Objects;

/**
 * Đại diện cho definition của segment (profile filter + event filter).
 * Thực chất là JSON DSL.
 */
public class SegmentDefinition {

    // Có thể dùng String (json) hoặc Map<String, Object>
    private final String rawJson;

    public SegmentDefinition(String rawJson) {
        this.rawJson = Objects.requireNonNull(rawJson);
    }

    public String getRawJson() {
        return rawJson;
    }

    // Sau này có thể thêm validate/parse ở đây
}
