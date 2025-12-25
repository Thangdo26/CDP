package com.vft.cdp.profile.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Raw profile data from client
 * Conforms to new simplified schema
 * tenant_id and app_id are injected from auth context (header), not from request body
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RawProfile {

    // ========== Fields from Auth Context (NOT in request body) ==========

    @JsonProperty("tenant_id")
    private String tenantId;  // Injected from auth header

    @JsonProperty("app_id")
    private String appId;  // Injected from auth header (single value)

    // ========== Required Fields from Request Body ==========

    private String type;  // Request type

    @JsonProperty("user_id")
    private String userId;  // User identifier (replaces profile_id)

    private Traits traits;  // User information (required)

    // ========== Optional Fields from Request Body ==========

    private Platforms platforms;  // Device/platform info

    private Campaign campaign;  // Campaign information

    private Map<String, Object> metadata;  // Additional data like scores

    // ========== Inner Classes ==========

    /**
     * User traits (personal information)
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Traits {
        @JsonProperty("full_name")
        private String fullName;

        @JsonProperty("first_name")
        private String firstName;

        @JsonProperty("last_name")
        private String lastName;

        private String idcard;  // CCCD

        @JsonProperty("old_idcard")
        private String oldIdcard;  // CMND

        private String phone;

        private String email;

        private String gender;

        private String dob;  // Format: yyyy-MM-dd

        private String address;

        private String religion;
    }

    /**
     * Device/Platform information
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Platforms {
        private String os;

        private String device;

        private String browser;

        @JsonProperty("app_version")
        private String appVersion;
    }

    /**
     * Campaign/UTM information
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Campaign {
        @JsonProperty("utm_source")
        private String utmSource;

        @JsonProperty("utm_campaign")
        private String utmCampaign;

        @JsonProperty("utm_medium")
        private String utmMedium;

        @JsonProperty("utm_content")
        private String utmContent;

        @JsonProperty("utm_term")
        private String utmTerm;

        @JsonProperty("utm_custom")
        private String utmCustom;
    }
}