package com.vft.cdp.inbound.domain;

import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.RawProfile;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

@Component
public class ProfileEnricher {

    public EnrichedProfile enrich(RawProfile raw) {

        String partitionKey = raw.getTenantId() + "|" + raw.getUserId();
        Instant now = Instant.now();
        OffsetDateTime nowOffset = OffsetDateTime.ofInstant(now, ZoneOffset.UTC);

        return EnrichedProfile.builder()
                // From raw profile
                .tenantId(raw.getTenantId())
                .appId(raw.getAppId())
                .type(raw.getType())
                .userId(raw.getUserId())
                .traits(raw.getTraits())
                .platforms(raw.getPlatforms())
                .campaign(raw.getCampaign())
                .metadata(raw.getMetadata())

                // Enrichment metadata
                .partitionKey(partitionKey)
                .enrichedAt(now)
                .enrichedId(UUID.randomUUID().toString())

                // Tracking timestamps
                .createdAt(nowOffset)
                .updatedAt(nowOffset)
                .firstSeenAt(nowOffset)
                .lastSeenAt(nowOffset)
                .version(1)

                .build();
    }
}