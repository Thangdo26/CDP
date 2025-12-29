package com.vft.cdp.profile.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.Map;
import  java.time.Instant;

/**
 * Profile ingestion request DTO
 * tenant_id and app_id are NOT in request body - they come from auth header
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProfileIngestionRequest {

    // ========== Required Fields ==========

    @NotEmpty(message = "type is required")
    private String type;

    @NotEmpty(message = "user_id is required")
    @JsonProperty("user_id")
    private String userId;

    @NotNull(message = "traits is required")
    @Valid
    private Traits traits;

    // ========== Optional Fields ==========

    private Platforms platforms;

    private Campaign campaign;

    private Map<String, Object> metadata;

    // ========== Inner Classes ==========

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