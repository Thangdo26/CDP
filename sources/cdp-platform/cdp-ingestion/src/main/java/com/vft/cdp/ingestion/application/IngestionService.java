package com.vft.cdp.ingestion.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vft.cdp.auth.domain.ApiKeyAuthContext;
import com.vft.cdp.common.event.IngestionEvent;
import com.vft.cdp.ingestion.application.dto.TrackRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.UUID;


@Service
@RequiredArgsConstructor
@Slf4j
public class IngestionService {

    private static final String TOPIC_INGESTION_RAW = "cdp.ingestion.raw";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public String ingestEvent(ApiKeyAuthContext authContext, TrackRequest req) {

        // 1. Build IngestionEvent từ request + auth context
        OffsetDateTime eventTime = req.getEventTime() != null
                ? req.getEventTime()
                : OffsetDateTime.now();

        IngestionEvent event = IngestionEvent.builder()
                .tenantId(authContext.getTenantId())
                .appId(authContext.getAppId())
                .type(req.getType())
                .eventName(req.getEvent())
                .userId(req.getUserId())
                .anonymousId(req.getAnonymousId())
                .eventTime(eventTime)
                .properties(req.getProperties())
                .traits(req.getTraits())
                .context(req.getContext())
                .build();

        // Optional: dùng userId / anonymousId làm key để đảm bảo ordering theo user
        String kafkaKey = event.getUserId() != null
                ? event.getUserId()
                : event.getAnonymousId();

        // 3. Push vào Kafka
        kafkaTemplate.send(TOPIC_INGESTION_RAW, kafkaKey, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send event to Kafka, topic={}, key={}",
                                TOPIC_INGESTION_RAW, kafkaKey, ex);
                    } else if (log.isDebugEnabled()) {
                        log.debug("Sent event to Kafka topic={}, key={}, partition={}, offset={}",
                                TOPIC_INGESTION_RAW,
                                kafkaKey,
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });

        // 4. Tạm trả về một requestId sinh ngẫu nhiên (sau này có thể dùng traceId)
        return UUID.randomUUID().toString();
    }
}
