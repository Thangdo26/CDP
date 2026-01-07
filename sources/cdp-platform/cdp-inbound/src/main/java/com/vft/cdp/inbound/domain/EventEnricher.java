package com.vft.cdp.inbound.domain;

import com.vft.cdp.common.event.EnrichedEvent;
import com.vft.cdp.common.event.IngestionEvent;
import java.time.Instant;
import java.util.UUID;

public class EventEnricher {

    public EnrichedEvent enrich(IngestionEvent src) {

        // 1. Identity resolution
        String profileId = (src.getUserId() != null && !src.getUserId().isBlank())
                ? "uid:" + src.getUserId()
                : "anon:" + src.getAnonymousId();

        // 2. Partition key
        String partitionKey = src.getTenantId() + "|" + profileId;

        // 3. Build EnrichedEvent bằng Lombok Builder
        return EnrichedEvent.builder()
                .tenantId(src.getTenantId())
                .appId(src.getAppId())
                .type(src.getType())
                .eventName(src.getEventName())
                .userId(src.getUserId())
                .anonymousId(src.getAnonymousId())
                .eventTime(src.getEventTime())
                .properties(src.getProperties())
                .traits(src.getTraits())
                .context(src.getContext())

                .profileId(profileId)
                .normalizedTime(Instant.now())
                .partitionKey(partitionKey)
                .enrichedId(UUID.randomUUID().toString())
                .build();
    }

}
