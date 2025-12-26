package com.vft.cdp.profile.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotEmpty;
import java.util.Map;

/**
 * Search profile request - NEW SCHEMA
 * Support multiple search criteria
 * All fields except tenant_id are optional
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SearchProfileRequest {

    // ========== Required Field ==========

    /**
     * Tenant ID (required for multi-tenancy)
     */
    @NotEmpty(message = "tenant_id is required")
    @JsonProperty("tenant_id")
    private String tenantId;

    // ========== Optional Search Criteria ==========

    /**
     * User ID (exact match)
     */
    @JsonProperty("user_id")
    private String userId;

    /**
     * App ID (exact match)
     */
    @JsonProperty("app_id")
    private String appId;

    /**
     * Request type (e.g., registration, profile_update)
     */
    private String type;  // NEW field

    /**
     * Search by traits fields
     * All fields are optional, can search by any combination
     */
    private TraitsSearch traits;  // NEW structured search

    /**
     * Search by metadata (generic key-value search)
     */
    private Map<String, Object> metadata;

    // ========== Pagination ==========

    /**
     * Page number (0-based)
     */
    @Min(0)
    @Builder.Default
    private Integer page = 0;

    /**
     * Page size (1-100)
     */
    @Min(1)
    @Max(100)
    @JsonProperty("page_size")
    @Builder.Default
    private Integer pageSize = 20;

    // ========== Sorting ==========

    /**
     * Sort field (e.g., user_id, created_at, updated_at, first_seen_at, last_seen_at)
     */
    @JsonProperty("sort_by")
    private String sortBy;

    /**
     * Sort direction: asc or desc
     */
    @JsonProperty("sort_order")
    @Builder.Default
    private String sortOrder = "desc";

    // ========== Inner Classes ==========

    /**
     * Traits search criteria
     * All fields are optional, search by any combination
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TraitsSearch {
        /**
         * Full name (partial match)
         */
        @JsonProperty("full_name")
        private String fullName;

        /**
         * First name (partial match)
         */
        @JsonProperty("first_name")
        private String firstName;

        /**
         * Last name (partial match)
         */
        @JsonProperty("last_name")
        private String lastName;

        /**
         * ID card / CCCD (exact match)
         */
        private String idcard;

        /**
         * Old ID card / CMND (exact match)
         */
        @JsonProperty("old_idcard")
        private String oldIdcard;

        /**
         * Phone number (exact match)
         */
        private String phone;

        /**
         * Email address (exact match)
         */
        private String email;

        /**
         * Gender
         */
        private String gender;

        /**
         * Date of birth (yyyy-MM-dd)
         */
        private String dob;

        /**
         * Address (partial match)
         */
        private String address;

        /**
         * Religion
         */
        private String religion;
    }
}