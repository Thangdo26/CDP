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
 * MASTER PROFILE DTO
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MasterProfileDTO {

    private String profileId;
    private String tenantId;
    private List<String> appId;

    private String status;
    private Boolean anonymous;

    private List<String> deviceId;
    private List<String> mergedIds;
    private MasterTraitsDTO traits;
    private List<String> segments;
    private Map<String, Double> scores;
    private Map<String, ConsentDTO> consents;

    private Instant createdAt;
    private Instant updatedAt;
    private Instant firstSeenAt;
    private Instant lastSeenAt;
    private List<String> sourceSystems;
    private Integer version;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MasterTraitsDTO {
        private List<String> email;
        private List<String> phone;
        private List<String> userId;
        private String firstName;
        private String lastName;
        private String gender;
        private String dob;
        private String country;
        private String city;
        private String address;
        private Double lastPurchaseAmount;
        private Instant lastPurchaseAt;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class ConsentDTO {
        private String status;
        private Instant updatedAt;
        private String source;
    }
}