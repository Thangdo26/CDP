package com.vft.cdp.ingestion.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
    @JsonProperty("user_id")
    private String userId;

    /**
     * anonymousId: device id, cookie id,...
     */
    @JsonProperty("anonymous_id")
    private String anonymousId;

    /**
     * Thời điểm event, nếu null sẽ default là now() trên server.
     */
    @JsonProperty("event_time")
    private String eventTime;

    /**
     * Thuộc tính event, ví dụ: price, currency, product_id,...
     */
    private Map<String, Object> properties;

    /**
     * Traits tạm thời gửi kèm (có thể được map vào profile).
     */
    private Map<String, Object> traits;


    /**
     * Thong tin device.
     */
    private Map<String, Object> device;


    /**
     * Thong tin chien dich marketing.
     */
    private Map<String, Object> campaign;


    /**
     * Context: ip, userAgent,...
     */
    private Map<String, Object> context;
}
