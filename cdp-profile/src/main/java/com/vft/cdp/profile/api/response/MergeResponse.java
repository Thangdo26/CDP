package com.vft.cdp.profile.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * Merge Response
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MergeResponse {

    /**
     * Response code
     * 200 = success, 400 = error
     */
    private int code;

    /**
     * Response message
     */
    private String message;

    /**
     * Created master profile ID
     */
    @JsonProperty("master_profile_id")
    private String masterProfileId;

    /**
     * Number of profiles merged
     */
    @JsonProperty("profiles_merged_count")
    private Integer profilesMergedCount;

    /**
     * Original profile IDs that were merged
     */
    @JsonProperty("merged_profile_ids")
    private List<String> mergedProfileIds;

    /**
     * Timestamp
     */
    @JsonProperty("merged_at")
    private Instant mergedAt;
}