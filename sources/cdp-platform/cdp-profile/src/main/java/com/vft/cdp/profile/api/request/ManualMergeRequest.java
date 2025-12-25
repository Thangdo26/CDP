package com.vft.cdp.profile.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Manual Merge Request
 *
 * Manually merge specified profiles
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ManualMergeRequest {

    /**
     * Tenant ID
     */
    @NotEmpty(message = "tenant_id is required")
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Profile IDs to merge
     * Format: "{tenant_id}|{app_id}|{user_id}"
     *
     * Example:
     * [
     *   "tenant-demo|app-demo|thang_test",
     *   "tenant-demo|app-demo|thang_test2"
     * ]
     */
    @NotEmpty(message = "profile_ids is required")
    @Size(min = 2, message = "At least 2 profiles required for merge")
    @JsonProperty("profile_ids")
    private List<String> profileIds;

    /**
     * Force merge (optional)
     * If true, merge even if profiles don't match duplicate criteria
     */
    @JsonProperty("force_merge")
    @Builder.Default
    private Boolean forceMerge = false;

    /**
     * Keep original profiles (optional)
     * If true, don't mark original profiles as merged
     */
    @JsonProperty("keep_originals")
    @Builder.Default
    private Boolean keepOriginals = false;
}