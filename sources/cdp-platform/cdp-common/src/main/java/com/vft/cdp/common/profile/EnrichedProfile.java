package com.vft.cdp.common.profile;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

/**
 * Enriched profile - NEW SCHEMA (khớp với RawProfile)
 * Đây là profile sau khi đã qua validation và enrichment
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
    private String userId;  // ← Đổi từ profileId thành userId

    private RawProfile.Traits traits;  // ← Structured traits

    private RawProfile.Platforms platforms;

    private RawProfile.Campaign campaign;

    private Map<String, Object> metadata;

    // ========== Enrichment Metadata (System adds) ==========
    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("enriched_at")
    private Instant enrichedAt;

    @JsonProperty("enriched_id")
    private String enrichedId;

    // ========== Tracking Metadata ==========
    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    @JsonProperty("first_seen_at")
    private Instant firstSeenAt;

    @JsonProperty("last_seen_at")
    private Instant lastSeenAt;

    private Integer version;
}