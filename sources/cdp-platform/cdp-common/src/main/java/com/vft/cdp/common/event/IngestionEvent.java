package com.vft.cdp.common.event;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.OffsetDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class IngestionEvent {

    private String tenantId;
    private String appId;

    private String type;          // track / identify
    private String eventName;     // "Add To Cart", "Purchase", etc.

    private String userId;        // external userId
    private String anonymousId;   // device id, cookie id, etc.

    private OffsetDateTime eventTime;

    private Map<String, Object> properties;
    private Map<String, Object> traits;
    private Map<String, Object> context;
}
