package com.vft.cdp.profile.api.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.vft.cdp.profile.application.dto.MasterProfileDTO;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * MASTER PROFILE RESPONSE
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * API response for master profile GET endpoint
 * Maps from MasterProfileDTO
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MasterProfileResponse {

    // ========== Identity ==========

    @JsonProperty("profile_id")
    private String profileId;

    @JsonProperty("tenant_id")
    private String tenantId;

    @JsonProperty("app_id")
    private List<String> appId;

    // ========== Status ==========

    private String status;

    private Boolean anonymous;

    // ========== Merged Profiles ==========

    @JsonProperty("device_id")
    private List<String> deviceId;

    @JsonProperty("merged_ids")
    private List<String> mergedIds;

    @JsonProperty("merge_count")
    private Integer mergeCount;

    // ========== Consolidated Data ==========

    private MasterTraitsResponse traits;

    private List<String> segments;

    private Map<String, Double> scores;

    private Map<String, ConsentResponse> consents;

    // ========== Timestamps ==========

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("updated_at")
    private Instant updatedAt;

    @JsonProperty("first_seen_at")
    private Instant firstSeenAt;

    @JsonProperty("last_seen_at")
    private Instant lastSeenAt;

    @JsonProperty("source_systems")
    private List<String> sourceSystems;

    private Integer version;

    // ========== Static Mapper from DTO ==========

    /**
     * Convert MasterProfileDTO to MasterProfileResponse
     */
    public static MasterProfileResponse fromDTO(MasterProfileDTO dto) {
        if (dto == null) return null;

        return MasterProfileResponse.builder()
                .profileId(dto.getProfileId())
                .tenantId(dto.getTenantId())
                .appId(dto.getAppId())
                .status(dto.getStatus())
                .anonymous(dto.getAnonymous())
                .deviceId(dto.getDeviceId())
                .mergedIds(dto.getMergedIds())
                .mergeCount(dto.getMergedIds() != null ? dto.getMergedIds().size() : 0)
                .traits(mapTraits(dto.getTraits()))
                .segments(dto.getSegments())
                .scores(dto.getScores())
                .consents(dto.getConsents() != null
                        ? dto.getConsents().entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey(),
                                e -> mapConsent(e.getValue())
                        ))
                        : null)
                .createdAt(dto.getCreatedAt())
                .updatedAt(dto.getUpdatedAt())
                .firstSeenAt(dto.getFirstSeenAt())
                .lastSeenAt(dto.getLastSeenAt())
                .sourceSystems(dto.getSourceSystems())
                .version(dto.getVersion())
                .build();
    }

    // ========== Private Mappers ==========

    private static MasterTraitsResponse mapTraits(MasterProfileDTO.MasterTraitsDTO traits) {
        if (traits == null) return null;

        return MasterTraitsResponse.builder()
                .email(traits.getEmail())
                .phone(traits.getPhone())
                .userId(traits.getUserId())
                .firstName(traits.getFirstName())
                .lastName(traits.getLastName())
                .gender(traits.getGender())
                .dob(traits.getDob())
                .country(traits.getCountry())
                .city(traits.getCity())
                .address(traits.getAddress())
                .lastPurchaseAmount(traits.getLastPurchaseAmount())
                .lastPurchaseAt(traits.getLastPurchaseAt())
                .build();
    }

    private static ConsentResponse mapConsent(MasterProfileDTO.ConsentDTO consent) {
        if (consent == null) return null;

        return ConsentResponse.builder()
                .status(consent.getStatus())
                .updatedAt(consent.getUpdatedAt())
                .source(consent.getSource())
                .build();
    }

    // ========== Inner Response Classes ==========

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MasterTraitsResponse {
        private List<String> email;
        private List<String> phone;

        @JsonProperty("user_id")
        private List<String> userId;

        @JsonProperty("first_name")
        private String firstName;

        @JsonProperty("last_name")
        private String lastName;

        private String gender;
        private String dob;
        private String country;
        private String city;
        private String address;

        @JsonProperty("last_purchase_amount")
        private Double lastPurchaseAmount;

        @JsonProperty("last_purchase_at")
        private Instant lastPurchaseAt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ConsentResponse {
        private String status;

        @JsonProperty("updated_at")
        private Instant updatedAt;

        private String source;
    }
}