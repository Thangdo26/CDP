package com.vft.cdp.segmentation.infra.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;

@Data
@Document(indexName = "cdp-segment-members")
public class SegmentMemberDocument {

    @Id
    private String id; // tenantId|segmentId|userId

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "segment_id")
    private String segmentId;

    @Field(type = FieldType.Keyword, name = "user_id")
    private String userId;

    @Field(type = FieldType.Date, name = "updated_at")
    private Instant updatedAt;
}
