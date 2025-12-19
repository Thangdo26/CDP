package com.vft.cdp.profile.application.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vft.cdp.common.profile.RawProfile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Profile DTO for application layer
 * Matches new profile schema structure
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProfileDto {

    // ========== Identity Fields ==========

    @JsonProperty("user_id")
    private String userId;  // Changed from profile_id

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("app_id")
    private String appId;  // Changed from List<String> to String

    // ========== Profile Data Fields ==========

    private String type;  // Request type (e.g., registration, profile_update)

    private RawProfile.Traits traits;  // Structured traits object

    private RawProfile.Platforms platforms;  // Device/platform information

    private RawProfile.Campaign campaign;  // UTM/campaign parameters

    private Map<String, Object> metadata;  // Additional metadata (scores, tags, etc.)

    // ========== System Metadata (Optional for response) ==========

    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("enriched_at")
    private Instant enrichedAt;

    @JsonProperty("enriched_id")
    private String enrichedId;

    @JsonProperty("created_at")
    private OffsetDateTime createdAt;

    @JsonProperty("updated_at")
    private OffsetDateTime updatedAt;

    @JsonProperty("first_seen_at")
    private OffsetDateTime firstSeenAt;

    @JsonProperty("last_seen_at")
    private OffsetDateTime lastSeenAt;

    private Integer version;
}