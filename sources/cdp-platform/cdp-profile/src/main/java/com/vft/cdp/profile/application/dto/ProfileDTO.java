package com.vft.cdp.profile.application.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE DTO
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProfileDTO {

    // Identity
    private String tenantId;
    private String appId;
    private String userId;
    private String type;

    // Status
    private String status;
    private String mergedToMasterId;
    private Instant mergedAt;
    private List<UserIdentityDTO> users;

    // Data
    private TraitsDTO traits;
    private PlatformsDTO platforms;
    private CampaignDTO campaign;
    private Map<String, Object> metadata;

    // Timestamps
    private Instant createdAt;
    private Instant updatedAt;
    private Instant firstSeenAt;
    private Instant lastSeenAt;
    private Integer version;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class UserIdentityDTO {
        private String appId;
        private String userId;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TraitsDTO {
        private String fullName;
        private String firstName;
        private String lastName;
        private String idcard;
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
    public static class PlatformsDTO {
        private String os;
        private String device;
        private String browser;
        private String appVersion;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class CampaignDTO {
        private String utmSource;
        private String utmCampaign;
        private String utmMedium;
        private String utmContent;
        private String utmTerm;
        private String utmCustom;
    }
}