package com.vft.cdp.segmentation.infra.es;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

@Data
@Document(indexName = "cdp-segments")
public class SegmentDocument {

    @Id
    private String id;  // segmentId

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "name")
    private String name;

    @Field(type = FieldType.Text, name = "description")
    private String description;

    @Field(type = FieldType.Text, name = "definition_json")
    private String definitionJson;

    @Field(type = FieldType.Keyword, name = "status")
    private String status;

    @Field(type = FieldType.Date, name = "created_at")
    private Instant createdAt;

    @Field(type = FieldType.Date, name = "updated_at")
    private Instant updatedAt;

    @Field(type = FieldType.Date, name = "last_built_at")
    private Instant lastBuiltAt;

    @Field(type = FieldType.Long, name = "estimated_size")
    private Long estimatedSize;
}
