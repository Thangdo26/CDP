package com.vft.cdp.profile.infra.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/*
 * Master Profile Elasticsearch Document
 */
@Data
@Document(indexName = "master_profiles")
public class MasterProfileDocument {

    @Id
    private String id;  // Same as profile_id

    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "app_id")
    private List<String> appId;

    @Field(type = FieldType.Keyword, name = "status")
    private String status;

    @Field(type = FieldType.Boolean, name = "anonymous")
    private Boolean anonymous;

    @Field(type = FieldType.Keyword, name = "device_id")
    private List<String> deviceId;

    @Field(type = FieldType.Keyword, name = "merged_ids")
    private List<String> mergedIds;

    // ========== TRAITS ==========
    @Field(type = FieldType.Object, name = "traits")
    private TraitsDocument traits;

    @Field(type = FieldType.Keyword, name = "segments")
    private List<String> segments;

    @Field(type = FieldType.Object, name = "scores")
    private Map<String, Double> scores;

    @Field(type = FieldType.Object, name = "consents")
    private Map<String, ConsentDocument> consents;

    @Field(type = FieldType.Object, name = "metadata")
    private MetadataDocument metadata;

    // ========== INNER DOCUMENTS ==========

    @Data
    public static class TraitsDocument {
        @Field(type = FieldType.Keyword, name = "email")
        private List<String> email;

        @Field(type = FieldType.Keyword, name = "phone")
        private List<String> phone;

        @Field(type = FieldType.Keyword, name = "user_id")
        private List<String> userId;

        @Field(type = FieldType.Keyword, name = "first_name")
        private String firstName;

        @Field(type = FieldType.Keyword, name = "last_name")
        private String lastName;

        @Field(type = FieldType.Keyword, name = "gender")
        private String gender;

        @Field(type = FieldType.Keyword, name = "dob")
        private String dob;

        @Field(type = FieldType.Keyword, name = "country")
        private String country;

        @Field(type = FieldType.Keyword, name = "city")
        private String city;

        @Field(type = FieldType.Text, name = "address")
        private String address;

        @Field(type = FieldType.Double, name = "last_purchase_amount")
        private Double lastPurchaseAmount;

        @Field(type = FieldType.Date, name = "last_purchase_at")
        private Instant lastPurchaseAt;
    }

    @Data
    public static class ConsentDocument {
        @Field(type = FieldType.Keyword, name = "status")
        private String status;

        @Field(type = FieldType.Date, name = "updated_at")
        private Instant updatedAt;

        @Field(type = FieldType.Keyword, name = "source")
        private String source;
    }

    @Data
    public static class MetadataDocument {
        @Field(type = FieldType.Date, name = "created_at")
        private Instant createdAt;

        @Field(type = FieldType.Date, name = "updated_at")
        private Instant updatedAt;

        @Field(type = FieldType.Date, name = "first_seen_at")
        private Instant firstSeenAt;

        @Field(type = FieldType.Date, name = "last_seen_at")
        private Instant lastSeenAt;

        @Field(type = FieldType.Keyword, name = "source_systems")
        private List<String> sourceSystems;

        @Field(type = FieldType.Integer, name = "version")
        private Integer version;
    }
}