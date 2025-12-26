package com.vft.cdp.profile.api.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.validation.Valid;
import java.util.Map;

/**
 * Update profile request DTO
 *
 * USAGE:
 * - Update existing profile fields
 * - Null fields will be ignored (won't overwrite existing data)
 * - Only non-null fields will be updated
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UpdateProfileRequest {

    /**
     * Request type (optional)
     */
    private String type;

    /**
     * User traits to update
     * Only non-null fields will be updated
     */
    @Valid
    private TraitsUpdate traits;

    /**
     * Platform information to update
     */
    private PlatformsUpdate platforms;

    /**
     * Campaign information to update
     */
    private CampaignUpdate campaign;

    /**
     * Additional metadata to update
     */
    private Map<String, Object> metadata;

    // ========== Inner Classes ==========

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TraitsUpdate {
        @JsonProperty("full_name")
        private String fullName;

        @JsonProperty("first_name")
        private String firstName;

        @JsonProperty("last_name")
        private String lastName;

        private String idcard;

        @JsonProperty("old_idcard")
        private String oldIdcard;

        private String phone;

        private String email;

        private String gender;

        private String dob;

        private String address;

        private String religion;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class PlatformsUpdate {
        private String os;
        private String device;
        private String browser;

        @JsonProperty("app_version")
        private String appVersion;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class CampaignUpdate {
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