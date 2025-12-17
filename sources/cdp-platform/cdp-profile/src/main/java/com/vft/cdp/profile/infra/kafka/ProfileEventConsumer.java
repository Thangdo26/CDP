package com.vft.cdp.profile.infra.kafka;

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

    @KafkaListener(
            topics = "${cdp.kafka.topics.events-enriched:cdp.events.enriched}",
            groupId = "${cdp.kafka.consumer.profile.group-id:cdp-profile}"
    )
    public void consume(EnrichedEvent event) {  // ✅ EnrichedEvent, KHÔNG PHẢI String

        log.info("=".repeat(60));
        log.info("🎯 PROFILE CONSUMER STARTED");
        log.info("TenantId: {}, ProfileId: {}, Event: {}",
                event.getTenantId(),
                event.getProfileId(),
                event.getEventName());

        try {
            profileService.upsertProfileFromEvent(event);
            log.info("✅ SUCCESS - Profile saved: {}", event.getProfileId());

        } catch (Exception ex) {
            log.error("❌ ERROR - Failed to process event", ex);
        }

        log.info("=".repeat(60));
    }
}