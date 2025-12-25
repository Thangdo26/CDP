package com.vft.cdp.profile.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Auto Merge Request
 *
 * Automatically detect and merge duplicate profiles in a tenant
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AutoMergeRequest {

    /**
     * Tenant ID to process
     */
    @NotEmpty(message = "tenant_id is required")
    @JsonProperty("tenant_id")
    private String tenantId;

    /**
     * Merge strategy (optional)
     * - "idcard_only": Only merge profiles with same idcard
     * - "phone_dob": Merge by phone + dob
     * - "email_name": Merge by email + full_name
     * - "all": Use all strategies (default)
     */
    @JsonProperty("merge_strategy")
    @Builder.Default
    private String mergeStrategy = "all";

    /**
     * Dry run mode (optional)
     * If true, only detect duplicates without actually merging
     */
    @JsonProperty("dry_run")
    @Builder.Default
    private Boolean dryRun = false;

    /**
     * Max groups to process (optional)
     * Useful for testing or processing in batches
     */
    @JsonProperty("max_groups")
    private Integer maxGroups;
}