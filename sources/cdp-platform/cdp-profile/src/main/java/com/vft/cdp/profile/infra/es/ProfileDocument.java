package com.vft.cdp.profile.infra.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;
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

    @Field(type = FieldType.Object, name = "identifiers")
    private Map<String, Object> identifiers;

    @Field(type = FieldType.Keyword, name = "status")
    private String status;

    @Field(type = FieldType.Object, name = "traits")
    private Map<String, Object> traits;

    @Field(type = FieldType.Date, name = "created_at")
    private Instant createdAt;

    @Field(type = FieldType.Date, name = "updated_at")
    private Instant updatedAt;
}
