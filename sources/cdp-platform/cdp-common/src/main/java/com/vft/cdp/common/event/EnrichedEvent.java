package com.vft.cdp.common.event;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedEvent {

    private String tenantId;
    private String appId;

    private String type;
    private String eventName;

    private String profileId;     // được mapping trong inbound-service
    private String userId;
    private String anonymousId;

    private Instant eventTime;
    private Instant normalizedTime;

    private Map<String, Object> properties;
    private Map<String, Object> traits;
    private Map<String, Object> context;
    private String partitionKey;
    private String enrichedId;
}
