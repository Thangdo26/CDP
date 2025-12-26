package com.vft.cdp.profile.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vft.cdp.common.profile.EnrichedProfile;
import com.vft.cdp.common.profile.RawProfile;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;  // ← ĐỔI IMPORT
import java.util.Map;

/**
 * Profile response DTO - FIXED TIMESTAMPS
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProfileResponse {

    // ========== Identity Fields ==========
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("app_id")
    private String appId;

    // ========== Profile Data ==========
    private String type;

    private TraitsResponse traits;

    private PlatformsResponse platforms;

    private CampaignResponse campaign;

    private Map<String, Object> metadata;

    // ========== Tracking Metadata ==========
    // ✅ ĐỔI TỪ OffsetDateTime → Instant
    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    @JsonProperty("first_seen_at")
    private Instant firstSeenAt;

    @JsonProperty("last_seen_at")
    private Instant lastSeenAt;

    private Integer version;

    // ========== Static Mapper (giữ nguyên logic) ==========
    public static ProfileResponse fromEnrichedProfile(EnrichedProfile profile) {
        if (profile == null) {
            return null;
        }

        return ProfileResponse.builder()
                .userId(profile.getUserId())
                .tenantId(profile.getTenantId())
                .appId(profile.getAppId())
                .type(profile.getType())
                .traits(mapTraits(profile.getTraits()))
                .platforms(mapPlatforms(profile.getPlatforms()))
                .campaign(mapCampaign(profile.getCampaign()))
                .metadata(profile.getMetadata())
                .createdAt(profile.getCreatedAt())
                .updatedAt(profile.getUpdatedAt())
                .firstSeenAt(profile.getFirstSeenAt())
                .lastSeenAt(profile.getLastSeenAt())
                .version(profile.getVersion())
                .build();
    }

    // Private mappers (giữ nguyên - không đổi)
    private static TraitsResponse mapTraits(RawProfile.Traits traits) {
        if (traits == null) {
            return null;
        }

        return TraitsResponse.builder()
                .fullName(traits.getFullName())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .idcard(traits.getIdcard())
                .oldIdcard(traits.getOldIdcard())
                .phone(traits.getPhone())
                .email(traits.getEmail())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .address(traits.getAddress())
                .religion(traits.getReligion())
                .build();
    }

    private static PlatformsResponse mapPlatforms(RawProfile.Platforms platforms) {
        if (platforms == null) {
            return null;
        }

        return PlatformsResponse.builder()
                .os(platforms.getOs())
                .device(platforms.getDevice())
                .browser(platforms.getBrowser())
                .appVersion(platforms.getAppVersion())
                .build();
    }

    private static CampaignResponse mapCampaign(RawProfile.Campaign campaign) {
        if (campaign == null) {
            return null;
        }

        return CampaignResponse.builder()
                .utmSource(campaign.getUtmSource())
                .utmCampaign(campaign.getUtmCampaign())
                .utmMedium(campaign.getUtmMedium())
                .utmContent(campaign.getUtmContent())
                .utmTerm(campaign.getUtmTerm())
                .utmCustom(campaign.getUtmCustom())
                .build();
    }

    // ========== Inner Response Classes (giữ nguyên) ==========

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class TraitsResponse {
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
    public static class PlatformsResponse {
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
    public static class CampaignResponse {
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