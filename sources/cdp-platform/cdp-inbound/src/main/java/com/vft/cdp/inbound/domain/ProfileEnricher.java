package com.vft.cdp.inbound.domain;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.RawProfile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

/**
 * Profile Enricher - FIXED TIMESTAMPS
 */
@Component
public class ProfileEnricher {

    public EnrichedProfile enrich(RawProfile raw) {

        String partitionKey = raw.getTenantId() + "|" + raw.getUserId();
        Instant now = Instant.now();  // ✅ Dùng Instant thay vì OffsetDateTime

        return EnrichedProfile.builder()
                // From raw
                .tenantId(raw.getTenantId())
                .appId(raw.getAppId())
                .type(raw.getType())
                .userId(raw.getUserId())
                .traits(raw.getTraits())
                .platforms(raw.getPlatforms())
                .campaign(raw.getCampaign())
                .metadata(raw.getMetadata())

                // System metadata
                .partitionKey(partitionKey)
                .enrichedAt(now)
                .enrichedId(UUID.randomUUID().toString())

                // Timestamps - tất cả dùng Instant
                .createdAt(now)
                .updatedAt(now)
                .firstSeenAt(now)
                .lastSeenAt(now)
                .version(1)

                .build();
    }
}