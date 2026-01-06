package com.vft.cdp.profile.infra.es.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.*;

import java.time.Instant;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE MAPPING DOCUMENT
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Maps (tenant_id, app_id, user_id) → profile_id in profiles_thang_dev
 *
 * PURPOSE:
 * - Multiple user identities can point to ONE unique profile
 * - Fast lookup by tenant_id + app_id + user_id
 * - Enables profile deduplication by idcard
 *
 * INDEX STRUCTURE:
 * - Primary key: tenant_id|app_id|user_id
 * - Foreign key: profile_id (points to profiles_thang_dev)
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(indexName = "profile_mapping")
//@Setting(
//        shards = 3,
//        replicas = 1,
//        refreshInterval = "1s"
//)
public class ProfileMappingDocument {

    /**
     * Document ID: tenant_id|app_id|user_id
     * This is the unique identifier for each mapping
     */
    @Id
    private String id;

    // ══════════════════════════════════════════════════════════
    // IDENTITY FIELDS (for lookup)
    // ══════════════════════════════════════════════════════════

    /**
     * Tenant ID - multi-tenancy support
     */
    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    /**
     * App ID - multiple apps per tenant
     */
    @Field(type = FieldType.Keyword, name = "app_id")
    private String appId;

    /**
     * User ID - identifier from source system
     */
    @Field(type = FieldType.Keyword, name = "user_id")
    private String userId;

    // ══════════════════════════════════════════════════════════
    // REFERENCE TO MASTER PROFILE
    // ══════════════════════════════════════════════════════════

    /**
     * Profile ID in profiles_thang_dev index
     * Format: can be idcard or generated UUID
     * This links to the unique profile document
     */
    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;

    // ══════════════════════════════════════════════════════════
    // METADATA
    // ══════════════════════════════════════════════════════════

    /**
     * When this mapping was created
     */
    @Field(type = FieldType.Date, name = "created_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant createdAt;

    /**
     * Last update timestamp
     */
    @Field(type = FieldType.Date, name = "updated_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant updatedAt;

    // ══════════════════════════════════════════════════════════
    // HELPER METHODS
    // ══════════════════════════════════════════════════════════

    /**
     * Build mapping document ID
     */
    public static String buildId(String tenantId, String appId, String userId) {
        return tenantId + "|" + appId + "|" + userId;
    }

    /**
     * Create a new mapping
     */
    public static ProfileMappingDocument create(
            String tenantId,
            String appId,
            String userId,
            String profileId
    ) {
        Instant now = Instant.now();
        return ProfileMappingDocument.builder()
                .id(buildId(tenantId, appId, userId))
                .tenantId(tenantId)
                .appId(appId)
                .userId(userId)
                .profileId(profileId)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}