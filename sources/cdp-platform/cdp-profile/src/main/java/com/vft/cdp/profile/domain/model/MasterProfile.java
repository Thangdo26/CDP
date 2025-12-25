package com.vft.cdp.profile.domain.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/*
 * Master Profile - Unified customer profile after merge
 *
 * CONCEPT:
 * - One customer may have multiple profiles across apps
 * - Master profile aggregates all identities
 * - Source profiles marked as "merged"
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MasterProfile {

    // ========== IDENTITY ==========
    private String profileId;           // Master profile ID: mp_{uuid}
    private String tenantId;            // Same tenant only
    private List<String> appId;         // All apps this customer used

    // ========== STATUS ==========
    private ProfileStatus status;       // active | merged | deleted | test
    private boolean anonymous;          // false for identified users

    // ========== IDENTIFIERS (Arrays) ==========
    private List<String> deviceId;      // All devices
    private List<String> mergedIds;     // Original profile IDs that were merged

    // ========== TRAITS ==========
    private MasterTraits traits;

    // ========== ENRICHMENT ==========
    private List<String> segments;      // Segment IDs
    private Map<String, Double> scores; // AI scores

    // ========== CONSENTS ==========
    private Map<String, Consent> consents;

    // ========== METADATA ==========
    private MasterMetadata metadata;

    // ========== INNER CLASSES ==========

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MasterTraits {
        // Arrays (aggregated from all profiles)
        private List<String> email;
        private List<String> phone;
        private List<String> userId;      // External user IDs

        // Single values (from latest profile)
        private String firstName;
        private String lastName;
        private String gender;
        private String dob;
        private String country;
        private String city;
        private String address;

        // Business metrics
        private Double lastPurchaseAmount;
        private Instant lastPurchaseAt;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Consent {
        private String status;          // granted | denied | pending
        private Instant updatedAt;
        private String source;          // web_form | api | app
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MasterMetadata {
        private Instant createdAt;
        private Instant updatedAt;
        private Instant firstSeenAt;
        private Instant lastSeenAt;
        private List<String> sourceSystems;  // web | app | api
        private Integer version;
    }

    public enum ProfileStatus {
        ACTIVE,
        MERGED,
        DELETED,
        TEST
    }
}