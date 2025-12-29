package com.vft.cdp.profile.infra.es.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.*;

import java.time.Instant;
import java.util.Map;

/**
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 * PROFILE ELASTICSEARCH DOCUMENT
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 *
 * Infrastructure layer - ES specific representation
 * ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Document(indexName = "profiles_thang_dev")
public class ProfileDocument {

    @Id
    private String id;  // tenant_id|app_id|user_id

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // IDENTITY
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "app_id")
    private String appId;

    @Field(type = FieldType.Keyword, name = "user_id")
    private String userId;

    @Field(type = FieldType.Keyword, name = "type")
    private String type;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // STATUS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Field(type = FieldType.Keyword, name = "status")
    private String status;

    @Field(type = FieldType.Keyword, name = "merged_to_master_id")
    private String mergedToMasterId;

    @Field(type = FieldType.Date, name = "merged_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant mergedAt;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // NESTED OBJECTS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Field(type = FieldType.Object, name = "traits")
    private Traits traits;

    @Field(type = FieldType.Object, name = "platforms")
    private Platforms platforms;

    @Field(type = FieldType.Object, name = "campaign")
    private Campaign campaign;

    @Field(type = FieldType.Object, name = "metadata")
    private Map<String, Object> metadata;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // TIMESTAMPS
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Field(type = FieldType.Date, name = "created_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant createdAt;

    @Field(type = FieldType.Date, name = "updated_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant updatedAt;

    @Field(type = FieldType.Date, name = "first_seen_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant firstSeenAt;

    @Field(type = FieldType.Date, name = "last_seen_at", format = DateFormat.date_hour_minute_second_millis)
    private Instant lastSeenAt;

    @Field(type = FieldType.Integer, name = "version")
    private Integer version;

    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    // INNER CLASSES
    // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Traits {
        @Field(type = FieldType.Text, name = "full_name")
        private String fullName;

        @Field(type = FieldType.Keyword, name = "first_name")
        private String firstName;

        @Field(type = FieldType.Keyword, name = "last_name")
        private String lastName;

        @Field(type = FieldType.Keyword, name = "idcard")
        private String idcard;

        @Field(type = FieldType.Keyword, name = "old_idcard")
        private String oldIdcard;

        @Field(type = FieldType.Keyword, name = "phone")
        private String phone;

        @Field(type = FieldType.Keyword, name = "email")
        private String email;

        @Field(type = FieldType.Keyword, name = "gender")
        private String gender;

        @Field(type = FieldType.Keyword, name = "dob")
        private String dob;

        @Field(type = FieldType.Text, name = "address")
        private String address;

        @Field(type = FieldType.Keyword, name = "religion")
        private String religion;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Platforms {
        @Field(type = FieldType.Keyword, name = "os")
        private String os;

        @Field(type = FieldType.Keyword, name = "device")
        private String device;

        @Field(type = FieldType.Keyword, name = "browser")
        private String browser;

        @Field(type = FieldType.Keyword, name = "app_version")
        private String appVersion;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class Campaign {
        @Field(type = FieldType.Keyword, name = "utm_source")
        private String utmSource;

        @Field(type = FieldType.Keyword, name = "utm_campaign")
        private String utmCampaign;

        @Field(type = FieldType.Keyword, name = "utm_medium")
        private String utmMedium;

        @Field(type = FieldType.Keyword, name = "utm_content")
        private String utmContent;

        @Field(type = FieldType.Keyword, name = "utm_term")
        private String utmTerm;

        @Field(type = FieldType.Keyword, name = "utm_custom")
        private String utmCustom;
    }
}