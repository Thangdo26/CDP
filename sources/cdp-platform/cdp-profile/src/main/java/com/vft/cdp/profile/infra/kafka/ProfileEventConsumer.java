package com.vft.cdp.profile.infra.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vft.cdp.common.event.EnrichedEvent;
import com.vft.cdp.profile.application.ProfileService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProfileEventConsumer {

    private final ProfileService profileService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${cdp.kafka.topics.events-enriched:cdp.events.enriched}",
            groupId = "${cdp.kafka.consumer.profile.group-id:cdp-profile}"
    )
    public void consume(String message) {
        try {
            EnrichedEvent event = objectMapper.readValue(message, EnrichedEvent.class);

            log.debug("Received enriched event: tenant={}, profileId={}",
                    event.getTenantId(), event.getProfileId());

            profileService.upsertProfileFromEvent(event);

        } catch (Exception ex) {
            log.error("Failed to process event: {}", message, ex);
        }
    }
}
