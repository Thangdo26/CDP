package com.vft.cdp.event.infra.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;  // ← Change from OffsetDateTime
import java.util.Map;

/**
 * Elasticsearch Document for Event
 *
 * WHY SEPARATE FROM DOMAIN?
 * - Domain: Pure business logic, no infrastructure
 * - Document: ES-specific annotations and types
 * - Separation: Easy to change DB without touching domain
 *
 * ANNOTATIONS EXPLAINED:
 * @Document: Mark as ES document
 * @Id: Primary key (_id in ES)
 * @Field: Map Java field → ES field
 */
@Data  // Lombok: getters, setters, toString, equals, hashCode
@Document(indexName = "events_v1")
public class EventDocument {

    @Id
    private String id;

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "app_id")
    private String appId;

    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;

    @Field(type = FieldType.Keyword, name = "event_name")
    private String eventName;

    @Field(type = FieldType.Date, name = "event_time",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant eventTime;

    @Field(type = FieldType.Date, name = "normalized_time",
            format = DateFormat.date_hour_minute_second_millis)
    private Instant normalizedTime;

    @Field(type = FieldType.Object, name = "properties")
    private Map<String, Object> properties;

    @Field(type = FieldType.Object, name = "traits")
    private Map<String, Object> traits;

    @Field(type = FieldType.Object, name = "context")
    private Map<String, Object> context;
}