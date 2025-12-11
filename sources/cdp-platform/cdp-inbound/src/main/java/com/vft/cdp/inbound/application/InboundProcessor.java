package com.vft.cdp.inbound.application;

import com.vft.cdp.common.event.EnrichedEvent;
import com.vft.cdp.common.event.IngestionEvent;
import com.vft.cdp.common.exception.EventValidationException;
import com.vft.cdp.inbound.domain.EventEnricher;
import com.vft.cdp.inbound.domain.EventValidator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InboundProcessor {

    private static final String ENRICHED_TOPIC = "cdp.events.enriched";

    private final EventValidator validator;
    private final EventEnricher enricher;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(
            topics = "${cdp.kafka.topics.ingestion-raw:cdp.ingestion.raw}",
            groupId = "${cdp.kafka.consumer.inbound.group-id:cdp-inbound}"
    )
    public void handleIngestionEvent(@Payload IngestionEvent event) {
        if (event == null) {
            log.warn("[inbound] Received null IngestionEvent");
            return;
        }

        log.debug("[inbound] Received IngestionEvent tenantId={}, appId={}, type={}, event={}",
                event.getTenantId(), event.getAppId(), event.getType(), event.getEventName());

        try {
            // 1. Validate cơ bản
            validator.validate(event);

            // 2. Enrich + map sang EnrichedEvent
            EnrichedEvent enriched = enricher.enrich(event);

            // 3. Push sang topic enriched
            String key = enriched.getPartitionKey();
            kafkaTemplate.send(ENRICHED_TOPIC, key, enriched)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("[inbound] Failed to publish EnrichedEvent, key={}", key, ex);
                        } else if (log.isDebugEnabled()) {
                            log.debug("[inbound] Published EnrichedEvent to {}, key={}, offset={}",
                                    ENRICHED_TOPIC,
                                    key,
                                    result.getRecordMetadata().offset());
                        }
                    });

        } catch (EventValidationException ex) {
            // Lỗi dữ liệu – log cảnh báo, không retry
            log.warn("[inbound] Drop invalid event tenantId={}, appId={}, reason={}",
                    event.getTenantId(), event.getAppId(), ex.getMessage());
        } catch (Exception ex) {
            // Lỗi hệ thống – cho phép retry / DLQ (sau này cấu hình thêm)
            log.error("[inbound] Unexpected error while processing IngestionEvent", ex);
            // tùy chiến lược retry, ở skeleton mình chỉ log
        }
    }
}
