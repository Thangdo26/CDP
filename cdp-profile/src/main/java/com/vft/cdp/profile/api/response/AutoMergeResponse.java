package com.vft.cdp.profile.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;


/**
 * Auto Merge Response
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AutoMergeResponse {

    /**
     * Response code
     */
    private int code;

    /**
     * Response message
     */
    private String message;

    /**
     * Tenant ID processed
     */
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Total duplicate groups found
     */
    @JsonProperty("duplicate_groups_found")
    private Integer duplicateGroupsFound;

    /**
     * Number of master profiles created
     */
    @JsonProperty("master_profiles_created")
    private Integer masterProfilesCreated;

    /**
     * Total profiles merged
     */
    @JsonProperty("total_profiles_merged")
    private Integer totalProfilesMerged;

    /**
     * Dry run mode
     */
    @JsonProperty("dry_run")
    private Boolean dryRun;

    /**
     * Details of each merge (optional)
     */
    @JsonProperty("merge_details")
    private List<MergeDetail> mergeDetails;

    /**
     * Processing time in milliseconds
     */
    @JsonProperty("processing_time_ms")
    private Long processingTimeMs;

    /**
     * Timestamp
     */
    @JsonProperty("processed_at")
    private Instant processedAt;

    /**
     * Detail of a single merge
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MergeDetail {
        @JsonProperty("master_profile_id")
        private String masterProfileId;

        @JsonProperty("match_strategy")
        private String matchStrategy;  // idcard | phone_dob | email_name

        @JsonProperty("profiles_merged")
        private List<String> profilesMerged;

        @JsonProperty("confidence")
        private String confidence;  // high | medium | low
    }
}