package com.vft.cdp.common.profile;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Enriched profile after validation and enrichment
 * Contains all RawProfile data plus enrichment metadata
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EnrichedProfile {

    // ========== From Auth Context ==========

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("app_id")
    private String appId;

    // ========== From Request Body ==========

    private String type;

    @JsonProperty("user_id")
    private String userId;

    private RawProfile.Traits traits;

    private RawProfile.Platforms platforms;

    private RawProfile.Campaign campaign;

    private Map<String, Object> metadata;  // Contains scores, etc.

    // ========== Enrichment Metadata (Added by system) ==========

    @JsonProperty("partition_key")
    private String partitionKey;  // Format: "{tenant_id}|{user_id}"

    @JsonProperty("enriched_at")
    private Instant enrichedAt;  // When profile was enriched

    @JsonProperty("enriched_id")
    private String enrichedId;  // Unique enrichment ID

    // ========== Tracking Metadata (System-managed) ==========

    @JsonProperty("created_at")
    private OffsetDateTime createdAt;  // First time profile was created

    @JsonProperty("updated_at")
    private OffsetDateTime updatedAt;  // Last update time

    @JsonProperty("first_seen_at")
    private OffsetDateTime firstSeenAt;  // First time user was seen

    @JsonProperty("last_seen_at")
    private OffsetDateTime lastSeenAt;  // Last time user was seen

    private Integer version;  // Profile version for optimistic locking
}