package com.vft.cdp.datagen.es;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.DateFormat;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.time.LocalDate;

@Data
@Document(indexName = "event-metrics-daily-v1")
public class EventMetricsDailyDocument {

    @Id
    private String id;

    @Field(type = FieldType.Keyword, name = "tenant_id")
    private String tenantId;

    @Field(type = FieldType.Keyword, name = "profile_id")
    private String profileId;

    @Field(type = FieldType.Keyword, name = "event_name")
    private String eventName;

    @Field(type = FieldType.Date, name = "event_date", format = DateFormat.date)
    private LocalDate eventDate;

    @Field(type = FieldType.Integer, name = "event_count")
    private Integer eventCount;

    @Field(type = FieldType.Double, name = "amount_sum")
    private Double amountSum;

    @Field(type = FieldType.Double, name = "amount_max")
    private Double amountMax;
}
