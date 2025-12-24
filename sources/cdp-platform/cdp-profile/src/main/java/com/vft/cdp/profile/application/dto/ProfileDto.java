package com.vft.cdp.profile.application.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vft.cdp.common.profile.RawProfile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;  // ← ĐỔI IMPORT
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProfileDto {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("app_id")
    private String appId;

    private String type;

    private RawProfile.Traits traits;

    private RawProfile.Platforms platforms;

    private RawProfile.Campaign campaign;

    private Map<String, Object> metadata;

    // System metadata
    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("enriched_at")
    private Instant enrichedAt;

    @JsonProperty("enriched_id")
    private String enrichedId;

    // ✅ ĐỔI TỪ OffsetDateTime → Instant
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