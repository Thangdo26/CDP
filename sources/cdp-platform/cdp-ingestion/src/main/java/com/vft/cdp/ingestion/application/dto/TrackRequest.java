package com.vft.cdp.ingestion.application.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TrackRequest {

    /**
     * Loại event: "track" / "identify" / ...
     */
    @NotBlank
    private String type;

    /**
     * Tên event, ví dụ: "ADD_TO_CART", "PURCHASE"
     */
    @NotBlank
    private String event;

    /**
     * user_id từ phía client (external id), có thể null nếu chỉ có anonymousId.
     */
    private String userId;

    /**
     * anonymousId: device id, cookie id,...
     */
    private String anonymousId;

    /**
     * Thời điểm event, nếu null sẽ default là now() trên server.
     */
    private OffsetDateTime eventTime;

    /**
     * Thuộc tính event, ví dụ: price, currency, product_id,...
     */
    private Map<String, Object> properties;

    /**
     * Traits tạm thời gửi kèm (có thể được map vào profile).
     */
    private Map<String, Object> traits;

    /**
     * Context: device, ip, userAgent,...
     */
    private Map<String, Object> context;
}
