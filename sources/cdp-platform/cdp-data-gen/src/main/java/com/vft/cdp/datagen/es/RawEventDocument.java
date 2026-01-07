package com.vft.cdp.datagen.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.Instant;
import java.util.Map;

@Data
@Document(indexName = "cdp-events-raw-*") // sẽ override bằng IndexCoordinates
public class RawEventDocument {

    @Id
    private String id;

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;

    @Field(type = FieldType.Keyword, name = "event_name")
    private String eventName;

    @Field(type = FieldType.Date, name = "event_time", format = DateFormat.date_hour_minute_second_millis)
    private Instant eventTime;

    @Field(type = FieldType.Keyword, name = "source")
    private String source;

    @Field(type = FieldType.Object, name = "properties")
    private Map<String, Object> properties;
}
