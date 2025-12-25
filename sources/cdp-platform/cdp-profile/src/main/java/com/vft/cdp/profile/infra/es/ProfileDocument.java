package com.vft.cdp.profile.infra.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.*;

import java.time.Instant;
import java.util.Map;

@Data
@Document(indexName = "profiles_thang_dev")
public class ProfileDocument {

    @Id
    private  String id;

    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;  // Format: "{tenant_id}|{app_id}|{user_id}"

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "app_id")
    private String appId;

    @Field(type = FieldType.Keyword, name = "user_id")
    private String userId;

    @Field(type = FieldType.Keyword, name = "type")
    private String type;

    // ========== Nested Traits Object ==========
    @Field(type = FieldType.Object, name = "traits")
    private TraitsDocument traits;

    @Field(type = FieldType.Object, name = "platforms")
    private PlatformsDocument platforms;

    @Field(type = FieldType.Object, name = "campaign")
    private CampaignDocument campaign;

    @Field(type = FieldType.Object, name = "metadata")
    private Map<String, Object> metadata;

    // ========== System Metadata ==========
    @Field(type = FieldType.Keyword, name = "partition_key")
    private String partitionKey;

    @Field(type = FieldType.Date, name = "enriched_at",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant enrichedAt;

    @Field(type = FieldType.Keyword, name = "enriched_id")
    private String enrichedId;

    // ========== Tracking Timestamps ==========
    @Field(type = FieldType.Date, name = "created_at",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant createdAt;

    @Field(type = FieldType.Date, name = "updated_at",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant updatedAt;

    @Field(type = FieldType.Date, name = "first_seen_at",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant firstSeenAt;

    @Field(type = FieldType.Date, name = "last_seen_at",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant lastSeenAt;

    @Field(type = FieldType.Integer, name = "version")
    private Integer version;

    // ========== Inner Documents ==========

    @Data
    public static class TraitsDocument {
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

        @Field(type = FieldType.Keyword, name = "dob")
        private String dob;

        @Field(type = FieldType.Keyword, name = "email")
        private String email;

        @Field(type = FieldType.Keyword, name = "gender")
        private String gender;

        @Field(type = FieldType.Text, name = "address")
        private String address;

        @Field(type = FieldType.Keyword, name = "religion")
        private String religion;
    }

    @Data
    public static class PlatformsDocument {
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
    public static class CampaignDocument {
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