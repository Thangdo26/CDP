package com.vft.cdp.profile.infra.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

@Data
@Document(indexName = "profiles_v1")
public class ProfileDocument {

    @Id
    private String id;  // tenantId|profileId

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;

    @Field(type = FieldType.Keyword, name = "app_id")
    private List<String> appId;

    @Field(type = FieldType.Keyword, name = "status")
    private String status;

    @Field(type = FieldType.Boolean, name = "anonymous")
    private Boolean anonymous;

    // Identities: nested object
    @Field(type = FieldType.Object, name = "identities")
    private Map<String, List<String>> identities;

    // Traits: flexible fields
    @Field(type = FieldType.Object, name = "traits")
    private Map<String, Object> traits;

    // Segments
    @Field(type = FieldType.Keyword, name = "segments")
    private List<String> segments;

    // Scores
    @Field(type = FieldType.Object, name = "scores")
    private Map<String, Double> scores;

    // Consents
    @Field(type = FieldType.Object, name = "consents")
    private Map<String, ConsentInfoDocument> consents;

    // Metadata
    @Field(type = FieldType.Object, name = "metadata")
    private ProfileMetadataDocument metadata;

    @Data
    public static class ConsentInfoDocument {
        @Field(type = FieldType.Keyword)
        private String status;

        @Field(type = FieldType.Date, name = "updated_at")
        private OffsetDateTime updatedAt;

        @Field(type = FieldType.Keyword)
        private String source;
    }

    @Data
    public static class ProfileMetadataDocument {
        @Field(type = FieldType.Date, name = "created_at")
        private OffsetDateTime createdAt;

        @Field(type = FieldType.Date, name = "updated_at")
        private OffsetDateTime updatedAt;

        @Field(type = FieldType.Date, name = "first_seen_at")
        private OffsetDateTime firstSeenAt;

        @Field(type = FieldType.Date, name = "last_seen_at")
        private OffsetDateTime lastSeenAt;

        @Field(type = FieldType.Keyword, name = "source_systems")
        private List<String> sourceSystems;

        @Field(type = FieldType.Integer)
        private Integer version;
    }
}