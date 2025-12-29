package com.vft.cdp.event.infra.kafka;

import com.vft.cdp.common.event.EnrichedEvent;
import com.vft.cdp.event.infra.es.EventDocument;
import com.vft.cdp.event.infra.es.EventMapper;
import com.vft.cdp.event.infra.es.SpringDataEventEsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka Consumer for storing events to Elasticsearch
 *
 * ARCHITECTURE:
 * Kafka Topic → Consumer → ES Storage
 *
 * WHY SEPARATE CONSUMER GROUP?
 * - Independent processing from profile consumer
 * - Different failure handling
 * - Can replay independently
 *
 * CONSUMER GROUPS IN SYSTEM:
 * cdp.events.enriched topic
 *   ├─ cdp-profile group → Update profiles
 *   └─ cdp-event-storage group → Store events (THIS)
 */
@Slf4j
@Component  // Spring: Auto-detect and register as bean
@RequiredArgsConstructor  // Lombok: Constructor injection
public class EventStorageConsumer {

    private final SpringDataEventEsRepository eventRepo;

    /**
     * Consume enriched events from Kafka
     *
     * @KafkaListener ANNOTATIONS EXPLAINED:
     *
     * topics: Which Kafka topic to consume from
     * - Uses Spring property placeholder ${...}
     * - Fallback value after colon: cdp.events.enriched
     *
     * groupId: Consumer group ID
     * - Multiple instances share the load
     * - Each message processed by one instance only
     *
     * @Payload: Message body (auto-deserialized by Spring Kafka)
     * - JsonDeserializer converts JSON → EnrichedEvent object
     *
     * @Header: Kafka metadata
     * - Can access partition, offset, timestamp, etc.
     */
    @KafkaListener(
            topics = "${cdp.kafka.topics.events-enriched:cdp.events.enriched}",
            groupId = "${cdp.kafka.consumer.event-storage.group-id:cdp-event-storage}"
    )
    public void consume(
            @Payload EnrichedEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

//        log.info("═".repeat(60));
        log.info("📥 EVENT STORAGE CONSUMER STARTED");
//        log.info("Event: tenant={}, profile={}, event={}",
//                event.getTenantId(),
//                event.getProfileId(),
//                event.getEventName());
        log.info("Kafka: partition={}, offset={}", partition, offset);

        try {
            // Convert EnrichedEvent → ES Document
            EventDocument doc = EventMapper.fromEnrichedEvent(event);

            // Save to Elasticsearch
            eventRepo.save(doc);

            log.info(" SUCCESS - Event stored: id={}", event.getEnrichedId());

        } catch (Exception ex) {
            log.error("❌ ERROR - Failed to store event: id={}",
                    event.getEnrichedId(), ex);

            // TODO: Implement retry logic or DLQ (Dead Letter Queue)
            // For now, just log error
        }

        log.info("═".repeat(60));
    }
}